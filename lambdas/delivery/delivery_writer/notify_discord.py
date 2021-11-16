# -*- coding: utf-8 -*-
import logging

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("event_dispatcher")
logger.setLevel(log_level)


def post_custom_bus_event(event_client, events):
    """Submit custom bus events."""
    try:
        if len(events) > 0:
            response = event_client.put_events(Entries=events)
        else:
            logger.info("No events to submit.")
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to submit events to event bridge. " + error_msg
        logger.error(message)
        return message
    else:
        if response and response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Submitted new event(s)")
        else:
            logger.warning("Bad response from event bridge " + str(response))
            message += "Failed to announce new players\n."
            logger.error(message)
