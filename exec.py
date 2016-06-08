#!/opt/SP/apps/python3.5/bin/python3

from subprocess import check_output
from puppet_enc import PuppetENC

import yaml
import json
import sys
import collections
import subprocess

#TODO: replace json directly to yaml

def hiera(key, certificate_name, merge=None, preserve_order=True, bin="/opt/SP/apps/ruby/current/bin/hiera", conf_file="/home/jboss/.puppet/hieradata/nodes/h.yaml"):
    if preserve_order:
        obj_pair_hook=collections.OrderedDict
    else:
        obj_pair_hook=None

    if merge==list:
        merge_opt='-a'
    elif merge==dict:
        merge_opt='-h'
    else:
        merge_opt=None

    args = [bin, key, 'certificate_name=%s' % certificate_name, '--config=%s' % conf_file]

    if merge_opt:
        args.append(merge_opt)

    o = check_output(args).rstrip()

    if o == 'nil':
        return None
    else:
        try:
            i = o.replace('=>', ':')
            return json.loads(i, object_pairs_hook=obj_pair_hook)
        except Exception as e:
            return o

def enc_by_source(node, source, environment):

    if source==b'application_database':
        enc = PuppetENC(hostname, port, db, db, service_name)
        return yaml.dump(enc.definition_by_certificate_name(sys.argv[1]), default_flow_style=False)


    elif source==b'hiera':
        return check_output(['hiera', node, 'environment=%s' % environment])

if __name__ == '__main__':
    environment = hiera("environment", sys.argv[1])
    source = hiera("source", sys.argv[1])
    print(enc_by_source(sys.argv[1], source, environment))
