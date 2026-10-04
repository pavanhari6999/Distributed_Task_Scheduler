import json, time, socket, sqlite3, threading, multiprocessing
from datetime import datetime
import psutil
from worker import worker_loop

HOST="127.0.0.1"
PORT=5050
DATABASE="scheduler.db"
PRIORITY={"HIGH":1,"MEDIUM":2,"LOW":3}

class MasterScheduler:
    def __init__(self):
        self.running=False
        self.server_socket=None
        self.workers={}
        self.tasks={}
        self.ready_queue=[]
        self.logs=[]
        self.lock=threading.Lock()
        self.task_counter=0
        self.algorithm="PRIORITY"
        self.quantum=500000
        self.auto_generation=False
        self.generator_thread=None
        self.create_database()

    def create_database(self):
        with sqlite3.connect(DATABASE) as c:
            c.execute("""CREATE TABLE IF NOT EXISTS task_history(
                id INTEGER,name TEXT,priority TEXT,status TEXT,worker TEXT,
                execution_time REAL,cpu_usage REAL,memory_usage REAL,
                completed_iterations INTEGER,total_iterations INTEGER,
                created_at TEXT,completed_at TEXT)""")

    def log(self,message):
        entry=f"{datetime.now():%H:%M:%S}  {message}"
        with self.lock:
            self.logs.insert(0,entry)
            self.logs=self.logs[:150]

    def start(self):
        if self.running:return
        self.running=True
        self.log("MASTER SCHEDULER STARTED")
        self.start_server()
        for i in range(1,4): self.start_worker(f"Worker-{i}")
        threading.Thread(target=self.dispatch_loop,daemon=True).start()
        threading.Thread(target=self.monitor_workers,daemon=True).start()
        self.log("3 real OS worker processes created")

    def start_server(self):
        self.server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.server_socket.bind((HOST,PORT))
        self.server_socket.listen(20)
        self.log(f"IPC SERVER LISTENING ON {HOST}:{PORT}")
        threading.Thread(target=self.accept_workers,daemon=True).start()

    def accept_workers(self):
        while self.running:
            try:
                client,_=self.server_socket.accept()
                threading.Thread(target=self.handle_worker,args=(client,),daemon=True).start()
            except Exception:
                if self.running: time.sleep(.5)

    def handle_worker(self,client):
        worker_id=None
        buffer=""
        try:
            while True:
                data=client.recv(4096)
                if not data: break
                buffer+=data.decode()
                while "\n" in buffer:
                    line,buffer=buffer.split("\n",1)
                    if not line.strip(): continue
                    msg=json.loads(line)
                    if msg.get("type")=="register":
                        worker_id=msg["worker_id"]
                        with self.lock:
                            w=self.workers.setdefault(worker_id,{
                                "id":worker_id,"pid":msg["pid"],"socket":client,
                                "process":None,"status":"IDLE","task":"-","cpu":0,
                                "memory":0,"alive":True,"manual_stop":False})
                            w.update(socket=client,pid=msg["pid"],alive=True)
                            if not w["manual_stop"]: w["status"]="IDLE"
                        self.log(f"{worker_id} CONNECTED | PID {msg['pid']}")
                    elif msg.get("type")=="slice_result":
                        self.process_slice(msg["data"])
        except Exception as e:
            self.log(f"Worker communication error: {e}")
        finally:
            if worker_id:
                with self.lock:
                    w=self.workers.get(worker_id)
                    if w:
                        w["socket"]=None
                        if not w["manual_stop"]:
                            w["alive"]=False; w["status"]="OFFLINE"

    def start_worker(self,worker_id):
        p=multiprocessing.Process(target=worker_loop,args=(worker_id,),name=worker_id,daemon=True)
        p.start()
        with self.lock:
            w=self.workers.setdefault(worker_id,{"id":worker_id})
            w.update(pid=p.pid,socket=None,process=p,status="STARTING",task="-",
                     cpu=0,memory=0,alive=True,manual_stop=False)
        self.log(f"{worker_id} STARTED | OS PID {p.pid}")

    def add_task(self,task_name,priority="MEDIUM",workload=5):
        with self.lock:
            self.task_counter+=1
            tid=self.task_counter
        priority=priority.upper()
        workload=max(1,min(int(workload),50))
        total=workload*1_000_000
        task={"id":tid,"name":task_name,"priority":priority,
              "priority_value":PRIORITY.get(priority,2),"total_iterations":total,
              "remaining_iterations":total,"completed_iterations":0,"progress":0,
              "status":"READY","worker":"-","execution_time":0,"cpu_usage":0,
              "memory_usage":0,"result":0,"created_at":datetime.now().strftime("%H:%M:%S"),
              "completed_at":"-"}
        with self.lock:
            self.tasks[tid]=task; self.ready_queue.append(tid)
        self.log(f"TASK #{tid} ARRIVED | {priority} | {task_name}")
        return task

    def select_next_task(self):
        with self.lock:
            ids=[i for i in self.ready_queue if i in self.tasks and self.tasks[i]["status"]=="READY"]
            if not ids:
                self.ready_queue.clear(); return None
            if self.algorithm=="PRIORITY":
                selected=min(ids,key=lambda i:(self.tasks[i]["priority_value"],i))
            elif self.algorithm=="FCFS":
                selected=min(ids)
            else:
                selected=ids[0]
            self.ready_queue.remove(selected)
            return self.tasks[selected]

    def find_worker(self):
        with self.lock:
            available=[w for w in self.workers.values()
                       if w["alive"] and not w["manual_stop"] and w["socket"] is not None and w["status"]=="IDLE"]
        return min(available,key=lambda w:w["cpu"]) if available else None

    def dispatch_loop(self):
        while self.running:
            worker=self.find_worker()
            task=self.select_next_task() if worker else None
            if not worker or not task:
                time.sleep(.05); continue
            try:
                size=min(self.quantum,task["remaining_iterations"])
                worker["socket"].sendall((json.dumps({"type":"execute","task_id":task["id"],
                    "task_name":task["name"],"start_iteration":task["completed_iterations"],
                    "slice_iterations":size})+"\n").encode())
                with self.lock:
                    task["status"]="RUNNING"; task["worker"]=worker["id"]
                    worker["status"]="RUNNING"; worker["task"]=f"Task #{task['id']}"
                self.log(f"SCHEDULER → {worker['id']} → Task #{task['id']} | {self.algorithm}")
            except Exception:
                with self.lock:
                    task["status"]="READY"; task["worker"]="-"; self.ready_queue.insert(0,task["id"])
                time.sleep(.1)

    def process_slice(self,r):
        tid,wid=r["task_id"],r["worker_id"]
        with self.lock:
            task=self.tasks.get(tid)
            if not task:return
            n=r["completed_iterations"]
            task["completed_iterations"]+=n
            task["remaining_iterations"]-=n
            task["execution_time"]+=r["execution_time"]
            task["cpu_usage"]=r["cpu_usage"]; task["memory_usage"]=r["memory_usage"]
            task["result"]+=r["partial_result"]
            task["progress"]=round(task["completed_iterations"]/task["total_iterations"]*100,1)
            if wid in self.workers:
                w=self.workers[wid]; w["cpu"]=r["cpu_usage"]; w["memory"]=r["memory_usage"]
                w["status"]="IDLE"; w["task"]="-"
            if task["remaining_iterations"]<=0:
                task["status"]="COMPLETED"; task["progress"]=100; task["worker"]=wid
                task["completed_at"]=datetime.now().strftime("%H:%M:%S"); completed=True
            else:
                task["status"]="READY"; task["worker"]="-"; self.ready_queue.append(tid); completed=False
        if completed:
            self.save_history(task); self.log(f"Task #{tid} COMPLETED by {wid}")
        else:self.log(f"Task #{tid} time slice completed → READY")

    def save_history(self,t):
        with sqlite3.connect(DATABASE) as c:
            c.execute("INSERT INTO task_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (t["id"],t["name"],t["priority"],t["status"],t["worker"],t["execution_time"],
                 t["cpu_usage"],t["memory_usage"],t["completed_iterations"],t["total_iterations"],
                 t["created_at"],t["completed_at"]))

    def set_algorithm(self,algorithm):
        algorithm=algorithm.upper()
        if algorithm not in ("PRIORITY","FCFS","ROUND_ROBIN"): return False
        with self.lock:self.algorithm=algorithm
        self.log(f"SCHEDULING ALGORITHM → {algorithm}"); return True

    def set_quantum(self,quantum):
        with self.lock:self.quantum=max(100000,min(int(quantum),2000000))
        self.log(f"TIME QUANTUM → {self.quantum:,} iterations")

    def toggle_auto_generation(self,enabled):
        self.auto_generation=enabled
        if enabled and (self.generator_thread is None or not self.generator_thread.is_alive()):
            self.generator_thread=threading.Thread(target=self.generate_tasks,daemon=True)
            self.generator_thread.start()
        self.log("AUTOMATIC TASK ARRIVAL ENABLED" if enabled else "AUTOMATIC TASK ARRIVAL DISABLED")

    def generate_tasks(self):
        import random
        names=["Matrix Multiplication","Prime Number Calculation","Numerical Analysis",
               "CPU Intensive Search","Data Processing","Hash Computation","Scientific Calculation"]
        while self.running and self.auto_generation:
            self.add_task(random.choice(names),random.choice(["HIGH","MEDIUM","LOW"]),random.randint(2,6))
            time.sleep(4)

    def monitor_workers(self):
        while self.running:
            time.sleep(.5)
            with self.lock: items=list(self.workers.items())
            for wid,w in items:
                p=w.get("process")
                if p and not p.is_alive() and not w["manual_stop"]:
                    self.handle_worker_failure(wid); continue
                try:
                    info=psutil.Process(w["pid"])
                    cpu=info.cpu_percent(interval=.02)
                    mem=round(info.memory_info().rss/1024/1024,2)
                    with self.lock:
                        if wid in self.workers:
                            self.workers[wid]["cpu"]=round(cpu,2); self.workers[wid]["memory"]=mem
                except Exception: pass

    def handle_worker_failure(self,worker_id):
        with self.lock:
            w=self.workers.get(worker_id)
            if not w:return
            failed=w["task"]; w["alive"]=False; w["status"]="FAILED"; w["socket"]=None; w["task"]="-"
        self.log(f"FAULT DETECTED → {worker_id}")
        if failed!="-":
            try:
                tid=int(failed.replace("Task #",""))
                with self.lock:
                    t=self.tasks.get(tid)
                    if t and t["status"]=="RUNNING":
                        t["status"]="READY"; t["worker"]="-"; self.ready_queue.insert(0,tid)
                self.log(f"Task #{tid} RECOVERED → READY QUEUE")
            except Exception: pass
        time.sleep(1); self.start_worker(worker_id)
        self.log(f"{worker_id} AUTOMATICALLY RESTARTED")

    def stop_worker(self,worker_id):
        with self.lock:
            w=self.workers.get(worker_id)
            if not w:return False
            w["manual_stop"]=True; p=w.get("process"); task=w["task"]
        if p and p.is_alive(): p.terminate(); p.join(timeout=1)
        with self.lock:
            w["alive"]=False; w["status"]="STOPPED"; w["socket"]=None; w["task"]="-"
        if task!="-":
            try:
                tid=int(task.replace("Task #",""))
                with self.lock:
                    t=self.tasks.get(tid)
                    if t and t["status"]!="COMPLETED":
                        t["status"]="READY"; t["worker"]="-"; self.ready_queue.insert(0,tid)
            except Exception: pass
        self.log(f"{worker_id} MANUALLY STOPPED"); return True

    def restart_worker(self,worker_id):
        with self.lock:
            w=self.workers.get(worker_id)
            if not w:return False
            p=w.get("process"); w["manual_stop"]=False
        if p and p.is_alive(): p.terminate(); p.join(timeout=1)
        self.start_worker(worker_id); self.log(f"{worker_id} MANUALLY RESTARTED"); return True

    def get_status(self):
        with self.lock:
            workers=[{"id":w["id"],"pid":w["pid"],"status":w["status"],"task":w["task"],
                      "cpu":round(w["cpu"],2),"memory":round(w["memory"],2),"alive":w["alive"]}
                     for w in self.workers.values()]
            tasks=list(self.tasks.values())
            stats={"total":len(tasks),
                   "ready":sum(t["status"]=="READY" for t in tasks),
                   "running":sum(t["status"]=="RUNNING" for t in tasks),
                   "completed":sum(t["status"]=="COMPLETED" for t in tasks)}
            return {"status":"RUNNING" if self.running else "STOPPED","algorithm":self.algorithm,
                    "quantum":self.quantum,"auto_generation":self.auto_generation,
                    "workers":workers,"tasks":tasks,"logs":self.logs,"statistics":stats}

def create_scheduler():
    s=MasterScheduler(); s.start(); return s
