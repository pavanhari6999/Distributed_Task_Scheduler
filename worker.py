import os
import socket
import json
import time
import psutil

HOST = "127.0.0.1"
PORT = 5050


def send_message(sock, data):
    sock.sendall((json.dumps(data) + "\n").encode())


def execute_time_slice(task_id, task_name, start_iteration, slice_iterations):
    """
    Executes a real CPU-bound workload inside an independent OS process.
    The scheduler controls how many iterations this process performs
    before returning control to the scheduler.
    """

    process = psutil.Process(os.getpid())

    process.cpu_percent(interval=None)

    start_time = time.time()

    total = 0
    end_iteration = start_iteration + slice_iterations

    for i in range(start_iteration, end_iteration):
        total += (i * i) % 1000003

    execution_time = round(time.time() - start_time, 4)

    cpu_usage = round(
        process.cpu_percent(interval=None),
        2
    )

    memory_usage = round(
        process.memory_info().rss / 1024 / 1024,
        2
    )

    return {
        "task_id": task_id,
        "task_name": task_name,
        "start_iteration": start_iteration,
        "completed_iterations": slice_iterations,
        "execution_time": execution_time,
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "partial_result": total
    }


def worker_loop(worker_id):

    worker_pid = os.getpid()

    while True:

        sock = None

        try:

            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            sock.settimeout(None)

            sock.connect(
                (HOST, PORT)
            )

            send_message(
                sock,
                {
                    "type": "register",
                    "worker_id": worker_id,
                    "pid": worker_pid
                }
            )

            buffer = ""

            while True:

                data = sock.recv(4096)

                if not data:
                    break

                buffer += data.decode()

                while "\n" in buffer:

                    line, buffer = buffer.split(
                        "\n",
                        1
                    )

                    if not line.strip():
                        continue

                    message = json.loads(line)

                    if message.get("type") == "execute":

                        result = execute_time_slice(
                            message["task_id"],
                            message["task_name"],
                            message["start_iteration"],
                            message["slice_iterations"]
                        )

                        result["worker_id"] = worker_id

                        send_message(
                            sock,
                            {
                                "type": "slice_result",
                                "data": result
                            }
                        )

        except Exception:

            time.sleep(1)

        finally:

            if sock:

                try:
                    sock.close()
                except Exception:
                    pass


if __name__ == "__main__":

    import sys

    worker_id = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "Worker-1"
    )

    worker_loop(worker_id)
