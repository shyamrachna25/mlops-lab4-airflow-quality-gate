# File: src/quality_gate.py
"""
THIS FILE IS THE CORE MODIFICATION FOR THIS SUBMISSION.

The original lab's DAG trains a model and always proceeds to loading and
"serving" it (triggering the Flask status app), with no check on whether
the model is actually any good. This module adds a real quality gate,
expressed through Airflow's own orchestration mechanism — a
BranchPythonOperator — rather than touching the model or dataset.

This mirrors the same "don't let a bad model through" pattern used in
this course's Docker lab (a build-stage validation gate) and MLflow lab
(a conditional promotion gate), now applied at the DAG-orchestration
level: a branch decision that routes the pipeline down a success path
or a rejection path based on a real, measured score.

The threshold is read from an Airflow Variable (MODEL_SCORE_THRESHOLD),
with a sane default, so it can be changed from the Airflow UI without
touching code — exactly how we flipped thresholds in the Docker and
MLflow labs to demonstrate both the PASS and FAIL paths.
"""

import logging

from airflow.models import Variable
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 0.85
PASS_TASK_ID = "load_model_task"
FAIL_TASK_ID = "model_rejected_task"


def check_model_quality(**kwargs) -> str:
    """
    BranchPythonOperator callable. Pulls the score computed by
    evaluate_model_task via XCom, compares it against the configured
    threshold, and returns the task_id of whichever branch should run
    next. Returning a task_id is how BranchPythonOperator decides which
    downstream path to take — every other direct downstream task is
    automatically marked 'skipped' (not failed).
    """
    ti = kwargs["ti"]
    score = ti.xcom_pull(task_ids="evaluate_model_task")

    threshold = float(Variable.get("MODEL_SCORE_THRESHOLD", default_var=DEFAULT_THRESHOLD))

    logger.info("Model score: %.4f | Required threshold: %.4f", score, threshold)

    if score >= threshold:
        logger.info("PASS — score meets threshold. Proceeding to load_model_task.")
        return PASS_TASK_ID
    else:
        logger.info("FAIL — score below threshold. Routing to model_rejected_task.")
        return FAIL_TASK_ID


def notify_rejection(**kwargs) -> None:
    """
    Reached only on the failing branch. Logs the rejection clearly and
    makes a best-effort attempt to send an email notification using
    Airflow's configured SMTP settings (airflow.cfg [smtp] section, as
    described in this lab's README).

    MODIFICATION (bug fix): the original lab's README describes
    notify_success/notify_failure callbacks and a send_email task that
    do not actually exist anywhere in the DAG file it ships — the
    EmailOperator is fully commented out. This function is what those
    described-but-missing notifications actually look like, wired up
    for real on the rejection path.

    Sending email is wrapped in try/except rather than left to fail the
    task outright: SMTP credentials are an external, environment-specific
    dependency (same category of scoping decision as, e.g., dropping
    Spark in the MLflow lab), and a missing SMTP config should not make
    this task — or the DAG run as a whole — show as failed when the
    actual quality-gate decision it's reporting was made correctly.
    """
    ti = kwargs["ti"]
    score = ti.xcom_pull(task_ids="evaluate_model_task")
    threshold = float(Variable.get("MODEL_SCORE_THRESHOLD", default_var=DEFAULT_THRESHOLD))

    message = (
        f"Model REJECTED by quality gate.\n"
        f"Score: {score:.4f}\n"
        f"Required threshold: {threshold:.4f}\n"
        f"The pipeline will NOT trigger the serving/status DAG for this run."
    )
    logger.warning(message)

    try:
        send_email(
            to=["rey.mhmmd@gmail.com"],
            subject="Airflow Lab4: Model rejected by quality gate",
            html_content=message.replace("\n", "<br>"),
        )
        logger.info("Rejection email sent successfully.")
    except Exception as e:
        # Deliberately caught: see docstring above.
        logger.warning("Could not send rejection email (SMTP likely not configured): %s", e)