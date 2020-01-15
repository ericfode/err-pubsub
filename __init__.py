
from errbot import BotPlugin, botcmd
import typing
import logging
log = logging.getLogger(__name__)

def _tag_subhook(func, project, topic, sub):
    log.info(f"webhooks:  Flag to bind {topic} to {getattr(func, '__name__', func)}")
    func._err_pubsub_topic = topic
    func._err_pubsub_sub = project
    func._err_pubsub_project = sub
    return func


def subhook(topic: str,
            project: str ,
            sub: str):
    """
    Decorator for webhooks

    :param uri_rule:
        The URL to use for this webhook, as per Flask request routing syntax.
        For more information, see:

        * http://flask.pocoo.org/docs/1.0/quickstart/#routing
        * http://flask.pocoo.org/docs/1.0/api/#flask.Flask.route
    :param methods:
        A tuple of allowed HTTP methods. By default, only GET and POST
        are allowed.
    :param form_param:
        The key who's contents will be passed to your method's `payload` parameter.
        This is used for example when using the `application/x-www-form-urlencoded`
        mimetype.
    :param raw:
        When set to true, this overrides the request decoding (including form_param) and
        passes the raw http request to your method's `payload` parameter.
        The value of payload will be a Flask
        `Request <http://flask.pocoo.org/docs/1.0/api/#flask.Request>`_.

    This decorator should be applied to methods of :class:`~errbot.botplugin.BotPlugin`
    classes to turn them into webhooks which can be reached on Err's built-in webserver.
    The bundled *Webserver* plugin needs to be configured before these URL's become reachable.

    Methods with this decorator are expected to have a signature like the following::

        @webhook
        def a_webhook(self, payload):
            pass
    """
    def wrapped_sub(func):
        return  _tag_subhook(func, project, topic, sub)
    return wrapped_sub
