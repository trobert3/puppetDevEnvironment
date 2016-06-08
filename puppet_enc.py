#!/opt/SP/apps/python3.5/bin/python3

import socket
import itertools
import sys
import re
import json
import yaml
from json.decoder import JSONDecodeError

from sqlalchemy import create_engine


class PuppetENC():
    def __init__(self, hostname, port, username, password, service_name):
        try:
            s = "oracle+cx_oracle://{username}:{password}" \
                "@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON)" \
                " (ADDRESS = (PROTOCOL = TCP)(HOST = {host})" \
                "(PORT = {port})) (CONNECT_DATA = " \
                "(SERVER = DEDICATED) (SERVICE_NAME = {service_name})))"

            self.engine = create_engine(s.format(username=username, password=password, host=hostname, port=port, service_name=service_name))

        except Exception as e:
            print(e)

    def certname_by_hostname(self, fqdn=None):
        if fqdn == None:
            fqdn = socket.getfqdn()
        result = self.engine.execute("select * from zones where HOSTNAME='" + fqdn +"'").first()

        return result.puppet_certname

    def _classes_by_certificate_name(self, puppet_certname):
        # TODO: Clean up query
        q = "SELECT PUPPET_CERTNAME, CLASS_NAME FROM ZONES " \
            "INNER JOIN PUPPET_CLASSES_MAPPINGS PCM ON PCM.ZONE_ID = ZONES.ID " \
            "INNER JOIN PUPPET_CLASSES PC ON PC.ID = PCM.CLASS_ID WHERE PUPPET_CERTNAME = '{puppet_certname}'"

        classes = []
        result = self.engine.execute(q.format(puppet_certname=puppet_certname))
        for row in result:
            classes.append(row['class_name'])
        return classes

    def _environment_by_certificate_name(self, puppet_certname):
        # This method returns either 'production', 'preproduction' or 'testing'
        # these are the ones that we support currently
        # TODO: Clean up query
        q = "SELECT E.NAME ENV_NAME, PUPPET_CERTNAME FROM CNAMES C INNER JOIN " \
            "APPLICATIONS A ON C.ID = A.CNAMES_ID INNER JOIN ZONES Z ON C.ZONES_ID = Z.ID " \
            "INNER JOIN ENVIRONMENTS E ON A.TYPE_ENVS_ID = E.ID WHERE PUPPET_CERTNAME = '{puppet_certname}'"
        result = self.engine.execute(q.format(puppet_certname=puppet_certname)).first()

        _response = 'preproduction'

        if result['env_name'] == 'PRD':
            _response = 'production'
        elif result['env_name'] == 'TESTING':
            _response = 'testing'

        return _response

    def _options_by_certificate_name(self, puppet_certname):
        # FIXME: make logic better. don't query and iterate twice

        options = {}
        result = self.engine.execute("select option_name, option_value from v_puppet_cert_view where PUPPET_CERTNAME='" + puppet_certname +"' and APP_ID is null")

        for row in result:
            try:
                value = json.loads(row['option_value'])
            except JSONDecodeError as e:
                value = row['option_value']

            if row['option_name'] in options:
                options[row['option_name']].append(value)
            else:
                options[row['option_name']] = [value]

        result = list(self.engine.execute("select option_name, option_value from v_puppet_cert_view where PUPPET_CERTNAME='" + puppet_certname +"' and APP_TYPE_ID is null"))

        for row in result:
            if row['option_name'] in options:
                options.pop(row['option_name'], None)

        for row in result:
            try:
                value = json.loads(row['option_value'])
            except JSONDecodeError as e:
                value = row['option_value']

            if row['option_name'] in options:
                options[row['option_name']].append(value)
            else:
                options[row['option_name']] = [value]

                
        return options

    def _run_q(self):
        return self.engine.execute()

    def definition_by_certificate_name(self, puppet_certname):
        definition = {}
        definition['classes'] = self._classes_by_certificate_name(puppet_certname)
        definition['parameters'] = self._options_by_certificate_name(puppet_certname)
        definition['environment'] = self._environment_by_certificate_name(puppet_certname)

        return definition


if __name__ == '__main__':
    enc = PuppetENC(hostname, port, db, db, service_name)
    print(yaml.dump(enc.definition_by_certificate_name(sys.argv[1]), default_flow_style=False))
