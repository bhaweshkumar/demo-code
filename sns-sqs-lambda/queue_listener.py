import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    SQS → Lambda event handler with partial batch response.
    - Logs every message to CloudWatch.
    - Any message that throws an exception is returned as a failure,
      so ONLY that message will be retried / moved to DLQ.
    """
    failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        body = record.get("body")

        try:
            # Log raw body
            logger.info(f"Received message {message_id}: {body}")

            # OPTIONAL: if your body is JSON, parse it here
            # data = json.loads(body)
            # logger.info(f"Parsed payload: {data}")

            # ---- YOUR PROCESSING LOGIC HERE ----
            # For demo: simulate failure if body contains the word "FAIL"
            if "FAIL" in (body or ""):
                raise ValueError("Simulated processing failure for testing DLQ")

            # If we reach here, this message is treated as successfully processed

        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            # Returning this ID tells Lambda/SQS: "this one failed, retry/redrive it"
            failures.append({"itemIdentifier": message_id})

    # Partial batch response format for SQS
    return {
        "batchItemFailures": failures
    }