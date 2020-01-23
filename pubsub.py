from inspect import getmembers, ismethod
from errbot import BotPlugin
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import typing
import typing_extensions as te


def _tag_subhook(func, project, sub):
    print(f"webhooks:  Flag to bind {sub} to {getattr(func, '__name__', func)}")
    func._err_pubsub_sub = sub
    func._err_pubsub_project = project
    return func


def subhook(project: str,
            sub: str):
    """
    Decorator for subscribers to google PubSub

    :param project:
        The google project in which the pubsub stream lives
    :param sub:
        The name of the subscription to use in the project

    This decorator allows one to react to a message on a google pub sub stream

    Methods with this decorator are expected to have a signature like the following::

        @subhook(project='a-test-project', sub='a-test-sub')
        def a_webhook(self, message):
            message.ack()
            pass
    """
    def wrapped_sub(func):
        return _tag_subhook(func, project, sub)
    return wrapped_sub


PubSubConfig = te.TypedDict('PubSubConfig',
                            {'SERVICE_ACCOUNT_JSON': typing.Optional[str]})


class Sub():
    def __init__(self, project, sub, callback):
        self.project: str = project
        self.sub = sub
        self.callback = callback
        self.subscription_name = None
        self.result = None
        self.activated = False

    def __hash__(self):
        return self.callback.__hash__()

    def __eq__(self, other):
        return self.callback == other.callback

    def activate(self, log, subscriber):
        if not self.activated:
            log.info("creating sub")
            log.info("activating sub")
            self.subscription_name = subscriber.subscription_path(self.project,
                                                                  self.sub)
            log.info(self.subscription_name)
            self.result = subscriber.subscribe(self.subscription_name,
                                               callback=self.callback)
            self.activated = True


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
        self.subs = set()
        super().__init__(*args, **kwargs)

    def get_configuration_template(self):
        return {'SERVICE_ACCOUNT_JSON': 'value'}

    def check_configuration(self, configuration):
        super().check_configuration(configuration)

    def configure(self, configuration) -> None:
        if configuration is not None:
            self.config = configuration
        if self.config is not None and 'SERVICE_ACCOUNT_JSON' in self.config:
            self.service_account_info = self.config['SERVICE_ACCOUNT_JSON']

    def reset_pubsub(self):
        """Zap everything for unittests"""
        # TODO: Maybe we should unsubsribe?
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subs = set()

    def find_subs(self, obj):
        """Checks a plugin for sns listeners and attaches callbacks if they are needed"""
        classname = obj.__class__.__name__
        self.log.info("Checking %s for pubsub hooks", classname)
        for name, func in getmembers(obj, ismethod):
            if getattr(func, '_err_pubsub_sub', False):
                self.log.info("pubsub routing %s, from %s",
                              func.__name__, func._err_pubsub_sub)
                new_sub = Sub(func._err_pubsub_project,
                              func._err_pubsub_sub, func)
                self.subs.add(new_sub)

    def activate(self):
        self.log.info('Starting PubSubListener')
        if self.service_account_info:
            creds = service_account.Credentials
            credentials = creds.from_service_account_file(
                self.service_account_info)
            self.subscriber = pubsub_v1.SubscriberClient(
                credentials=credentials)
        else:
            self.subscriber = pubsub_v1.SubscriberClient()
        super().activate()
        pm = self._bot.plugin_manager
        plugs = pm.get_all_active_plugins()
        if plugs:
            for p in plugs:
                self.find_subs(p)

        self.log.info("subs %s" , self.subs)
        if self.subs is not None:
            for sub in self.subs:
                self.log.info('listening to sub: {sub}'.format(sub=sub.subscription_name))
                try:
                    sub.activate(self.log, self.subscriber)
                except Exception as e:
                    self.log.info("there was a problem")
                    self.log.exception(e)
                    self.log.exception('Starting subscriber failed')
