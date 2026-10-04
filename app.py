from flask import (
    Flask,
    render_template,
    jsonify,
    request
)

import multiprocessing
import logging
import os

from scheduler import create_scheduler


app = Flask(__name__)

scheduler = None


logging.getLogger(
    "werkzeug"
).setLevel(
    logging.ERROR
)


# ---------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------

@app.route("/")
def home():

    return render_template(
        "index.html"
    )


# ---------------------------------------------------------
# SYSTEM STATUS
# ---------------------------------------------------------

@app.route("/api/status")
def status():

    if scheduler is None:

        return jsonify({
            "status": "STARTING"
        })

    return jsonify(
        scheduler.get_status()
    )


# ---------------------------------------------------------
# ADD REAL TASK
# ---------------------------------------------------------

@app.route(
    "/api/add_task",
    methods=["POST"]
)
def add_task():

    data = request.get_json() or {}

    name = data.get(
        "task_name",
        "CPU Task"
    )

    priority = data.get(
        "priority",
        "MEDIUM"
    )

    workload = int(
        data.get(
            "workload",
            5
        )
    )

    task = scheduler.add_task(
        name,
        priority,
        workload
    )

    return jsonify(task)


# ---------------------------------------------------------
# CHANGE SCHEDULER
# ---------------------------------------------------------

@app.route(
    "/api/algorithm",
    methods=["POST"]
)
def algorithm():

    data = request.get_json() or {}

    algorithm = data.get(
        "algorithm",
        "PRIORITY"
    )

    scheduler.set_algorithm(
        algorithm
    )

    return jsonify({
        "success": True
    })


# ---------------------------------------------------------
# QUANTUM
# ---------------------------------------------------------

@app.route(
    "/api/quantum",
    methods=["POST"]
)
def quantum():

    data = request.get_json() or {}

    value = int(
        data.get(
            "quantum",
            500000
        )
    )

    scheduler.set_quantum(
        value
    )

    return jsonify({
        "success": True
    })


# ---------------------------------------------------------
# AUTOMATIC TASK ARRIVAL
# ---------------------------------------------------------

@app.route(
    "/api/auto",
    methods=["POST"]
)
def auto():

    data = request.get_json() or {}

    enabled = bool(
        data.get(
            "enabled",
            False
        )
    )

    scheduler.toggle_auto_generation(
        enabled
    )

    return jsonify({
        "success": True,
        "enabled": enabled
    })


# ---------------------------------------------------------
# STOP WORKER
# ---------------------------------------------------------

@app.route(
    "/api/worker/<worker_id>/stop",
    methods=["POST"]
)
def stop_worker(worker_id):

    result = scheduler.stop_worker(
        worker_id
    )

    return jsonify({
        "success": result
    })


# ---------------------------------------------------------
# RESTART WORKER
# ---------------------------------------------------------

@app.route(
    "/api/worker/<worker_id>/restart",
    methods=["POST"]
)
def restart_worker(worker_id):

    result = scheduler.restart_worker(
        worker_id
    )

    return jsonify({
        "success": result
    })


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":

    multiprocessing.freeze_support()

    scheduler = create_scheduler()

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5001
            )
        ),
        debug=False,
        use_reloader=False
    )
