import collectd

from decimal import Decimal


class ARCstats(object):

    def __init__(self):
        self.plugin_name = "arcstats_ZoL"
        self.verbose_logging = False

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        collectd.info('%s plugin [verbose]: %s' % self.plugin_name, msg)

    def configure_callback(self, conf):
        """Receive configuration block"""
        for node in conf.children:
            if node.key == 'Verbose':
                self.verbose_logging = bool(node.values[0])
            else:
                collectd.warning('%s plugin: Unknown config key: %s.' % self.plugin_name, node.key)

    def dispatch_value(self, plugin_instance, value_type, instance, value):
        """Dispatch a value to collectd"""
        self.log_verbose('Sending value: %s.%s.%s=%s' % (self.plugin_name, plugin_instance, instance, value))
        val = collectd.Values()
        val.plugin = self.plugin_name
        val.plugin_instance = plugin_instance
        val.type = value_type
        val.type_instance = instance
        val.values = [value, ]
        val.dispatch()

    def fetch_info(self):
        """Fetch arcstats from proc filesystem"""
        k = [line.strip() for line in open('/proc/spl/kstat/zfs/arcstats')]

        if not k:
            self.log_verbose('%s plugin: No stats found. Is this ZFS on Linux?' % self.plugin_name)

        del k[0:2]
        kstat = {}

        for s in k:
            if not s:
                continue

            name, unused, value = s.split()
            kstat[name] = Decimal(value)

        return kstat

    def read_callback(self):
        """Collectd read callback"""
        self.log_verbose('Read callback called')
        kstat = self.fetch_info()
        if not kstat:
            self.log_verbose('%s plugin: No info received.' % self.plugin_name)
            return

        for n, v in kstat.items():
            self.dispatch_value('zfs', 'counter', n, v)


arc = ARCstats()
# register callbacks
collectd.register_config(arc.configure_callback)
collectd.register_read(arc.read_callback)
