
from inspect import getmembers, ismethod
from errbot import BotPlugin, botcmd
import errbot
from google.cloud import pubsub_v1
from google.auth import jwt
import json
import typing
import typing_extensions as te
import logging
log = logging.getLogger(__name__)
from typing import Callable


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

APubSub = te.TypedDict('APubSub', {'PROJECT': str, 'SUBSCRIPTION': str, 'TOPIC':str})

PubSubConfig = te.TypedDict('PubSubConfig', {'SERVICE_ACCOUNT_JSON': typing.Optional[str] }) 


class Sub():
    def __init__(self, project, topic, sub, callback: typing.Callable[[str], None]):
        self.topic_name: str ='projects/{project_id}/topics/{topic}'.format(
            project_id=project,
            topic=topic) 
        self.subscription_name: str='projects/{project_id}/subscriptions/{sub}'.format(
            project_id=project,
            sub=sub)
        self.project: str  = project
        self.callback: typing.Callable[[str], None] = callback

    def activate(self, subscriber):
        subscriber.create_subscription(
            name= self.subscription_name,
            topic= self.topic_name
        )
        subscriber.subscribe(self.subscription_name,
                            self.callback)


class PubSub(BotPlugin):
    """
    This plugin allows errbot to react to pubsub messages.
    It is not designed to act as a message filter so please only send
    messages you want errbot to react to most of the time.
    """
    def __init__(self, *args, **kwargs):
        self.subClient = None
        self.config = None
        self.audience = None
        self.service_account_info = None
        self.subscriber = None
        self.subs = []
        super().__init__(*args, **kwargs)
    

    def get_configuration_template(self) -> PubSubConfig:
       return {'SERVICE_ACCOUNT_JSON': None }

    def check_configuration(self, configuration):
        if not isinstance(configuration , PubSubConfig):
            raise errbot.ValidationException('PubSubConfig is broken')
        super().check_configuration(configuration)

    def configure(self, configuration: typing.Mapping) -> None:
        self.config = configuration
        if 'SERVICE_ACCOUNT_JSON' in self.config:
            self.service_account_info = json.load(open(self.config['SERVICE_ACCOUNT_JSON']))
        self.audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

    def reset_pubsub(self):
        """Zap everything for unittests"""
        # TODO: Maybe we should unsubsribe?
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subs = []

    def find_subs(self, obj):
        """Checks a plugin for sns listeners and attaches callbacks if they are needed"""
        classname = obj.__class__.__name__
        self.log.info("Checking %s for pubsub hooks", classname)
        for name, func in getmembers(obj, ismethod):
            if getattr(func, '_err_pubsub_topic', False): # False is the default value
                self.log.info("pubsub routing %s, from %s.%s",
                              func.__name__,
                              func._err_pubsub_topic,
                              func._err_pubsub_sub)
                new_sub = Sub(
                    func._err_pubsub_topic,
                    func._err_pubsub_sub,
                    func._err_pubsub_project,
                    func)
                self.subs.append(new_sub)

    def activate(self):
        self.log.info('Starting PubSubListener')
        self.subscriber = pubsub_v1.SubscriberClient()
        super().activate()
        pm = self._bot.plugin_manager
        plugs = pm.get_all_active_plugins()
        for p in plugs:
            self.find_subs(p)

        if self.sub is not None:
            for sub in self.subs:
                self.log.info('listening to topic: {topic} and sub: {sub}'.format(
                    topic=sub.topic_name, sub=sub.subscription_name))
                try:
                    sub.activate(self.subscriber)
                except Exception:
                    self.log.exception('Starting subscriber failed')