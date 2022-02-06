from ansible.module_utils.basic import AnsibleModule
from ansible_runner import get_inventory
from dns import resolver
from re import match, search
import os
import hashlib
import shlex

# Head of the base
# __root__ is a keyword for futher replacement
base_template_head = """<?xml version="1.0" encoding="utf-8"?>
<RDCMan programVersion="2.7" schemaVersion="3">
  <file>
    <credentialsProfiles />
    <properties>
      <expanded>True</expanded>
      <name>__root__</name>
    </properties>
"""
# Tail of the base
base_template_tail = """  </file>
  <connected />
  <favorites />
  <recentlyUsed />
</RDCMan>
"""
# Template of connections settings with _keywords_ for futher replacement
connection_set = """        <connectionSettings inherit="None">
          <connectToConsole>False</connectToConsole>
          <startProgram />
          <workingDir />
          <port>__port__</port>
          <loadBalanceInfo />
        </connectionSettings>
"""
# Template of group with _keywords_ for futher replacement
group_template = """    <group>
      <properties>
        <expanded>False</expanded>
        <name>__group_name__</name>
      </properties>
      <group>
        <properties>
          <expanded>False</expanded>
          <name>SR</name>
        </properties>
        __SR__
      </group>
      <group>
        <properties>
          <expanded>False</expanded>
          <name>PC</name>
        </properties>
        __PC__
      </group>
    </group>
"""
# Template of host with _keywords_ for futher replacement
server_template = """      <server>
        <properties>
          <displayName>__display_name__</displayName>
          <name>__server_name__</name>
        </properties>
      </server>
"""

ser_collector = []
pc_collector = []
org_collector = []


# Calculate file's hash
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.digest()



def parser(i_dir):
    i_files = os.listdir(i_dir)

    res = {
        'hosts':{},
        'groups':{}
    }

    for i_file in i_files:
        if 'ORG' in i_file:
            print()
        with open('{inv_dir}{inv_file}'.format(inv_dir=i_dir, inv_file=i_file)) as F:
            lines = F.readlines()
        i = 0
        while i < len(lines):
            if i == 253:
                print()
            if match(r'^\[.+:children\]', lines[i]):
                g_name = search(r'(?<=^\[).+(?=:children])', lines[i]).group()
                #print(g_name)
                res['groups'][g_name] = {
                    'children': []
                }
                i += 1
                while i < len(lines) and match(r'^[ a-zA-Z0-9]', lines[i]):
                    if lines[i][0] == ' ':
                        continue
                    org_name = shlex.split(lines[i].strip('\n\r'))[0]
                    res['groups'][g_name]['children'].append(org_name)
                    i += 1
            elif match(r'^\[.+\]', lines[i]):
                g_name = search(r'(?<=^\[).+(?=])', lines[i]).group()
                #print(g_name)
                res['groups'][g_name] = {
                    'hosts': []
                }
                i += 1
                while i < len(lines) and ((match(r'^[\na-zA-Z0-9]', lines[i]) or 
                len(lines[i]) == 0)):
                    if match(r'^[\n]', lines[i]):
                        i += 1
                        continue
                    attrs = shlex.split(lines[i].strip('\r\n'))
                    if res['hosts'].get(attrs[0]) == None:
                        res['hosts'][attrs[0]] = {
                            'vars': {}
                        }
                    if attrs[0] == 'tm-v-pc-d3w10.es.efsystem.ru':
                        print()
                    if len(attrs) > 1:
                        for k in range(1, len(attrs)):
                            key, val = attrs[k].split('=')
                            res['hosts'][attrs[0]]['vars'][key] = val
                    res['groups'][g_name]['hosts'].append(attrs[0])
                    i += 1
            else:
                i += 1
    return res

# Get module's output dict and path inventory(InventoryManager class)
# Returns string of the base
def create_base(result, inventory, base_name):
    base = base_template_head.replace('__root__', base_name)
    
    res = resolver.Resolver()
    res.timeout = 5
    result['dns_servers'] = res.nameservers
    for org in inventory['groups']['org']['children']:
        if inventory['groups'].get(org) == None:
            continue
        org_tmp = group_template
        org_tmp = org_tmp.replace('__group_name__', org.split('-')[1])    
        for host in inventory['groups'][org]['hosts']:
            try:
                if (not host in inventory['groups']['o-win']['hosts'] or
                    host in inventory['groups']['st-END']['hosts'] or 
                    host in inventory['groups']['st-INIT']['hosts'] or 
                    host in inventory['groups']['st-NONE']['hosts'] or
                    host in inventory['groups']['st-TEST']['hosts']):
                    continue
            except KeyError:
                pass

            if (inventory['hosts'].get(host) != None and
                inventory['hosts'][host]['vars'].get('ansible_host') != None):
                ip = inventory['hosts'][host]['vars']['ansible_host']
            else:
                if host in inventory['groups']['a-STAT']['hosts']:
                    can_resolve = False
                    try_count = 0
                    while not can_resolve and try_count < 2:
                        try:
                            ip = res.resolve(host)[0].address
                            can_resolve = True
                        except resolver.NXDOMAIN:
                            ip = 'NXDOMAIN'
                            break
                        except resolver.LifetimeTimeout:
                            can_resolve = False
                        finally:
                            try_count += 1
                    if not can_resolve:
                        result['failed_to_resolve'].append("Wasn't resolve {hostname}".format(hostname=host))                            
                elif host in inventory['groups']['a-DYN']['hosts']:
                    ip = host

            ser_tmp = server_template.replace(
                "__display_name__</displayName>\n          <name>__server_name__",
                "{hostname}</displayName>\n          <name>{ip_add}".format(hostname=host.split('.')[0].lower(), ip_add=ip)
                )
            # If host has RDP_PORT attribute add connection settings XML template
            if (inventory['hosts'].get(host) != None and
                inventory['hosts'][host].get('RDP_PORT') != None):
                ser_tmp = ser_tmp.replace(
                    '</server>', 
                    "{conn_set}      </server>".format(conn_set=connection_set.replace('__port__', str(inventory['hosts'][host]['RDP_PORT'])))
                    )
            # if host in c-SR group add it in ser_collector
            # if in c-PC - add in pc_collector
            if host in inventory['groups']['c-SR']['hosts']:
                ser_collector.append(ser_tmp)
            elif host in inventory['groups']['c-PC']['hosts']:
                pc_collector.append(ser_tmp)
        
        org_tmp = org_tmp.replace('__SR__', ''.join(ser_collector))
        org_tmp = org_tmp.replace('__PC__', ''.join(pc_collector))
        ser_collector.clear()
        pc_collector.clear()
        org_collector.append(org_tmp)
        
    base += '\n'.join(org_collector)
    org_collector.clear()
    base += base_template_tail
    return base

        
def main():
    # Define module args and check mode support
    module = AnsibleModule(
        argument_spec=dict(
            inventory_dir=dict(required=True),
            base_dir=dict(required=True),
            base_name=dict(required=True)
        ),
        supports_check_mode=True
    )
    # Define module's output
    result = dict(
        changed=False,
        message='',
        failed_to_resolve=[]
    )
    # Exit if module run in check mode
    if module.check_mode:
        module.exit_json(**result)
    
    inventory_dir = module.params['inventory_dir']
    base_dir = module.params['base_dir']
    base_name = module.params['base_name']

    inventory = parser(inventory_dir)
    base = create_base(result, inventory, base_name)
    filepath = '{b_dir}{b_name}.rdg'.format(b_dir=base_dir, b_name=base_name)
    if not os.path.exists(filepath):
        with open(filepath, 'w') as FILE:
            FILE.write(base)
        result['changed'] = True
    else:
        prev_hash = md5(filepath)
        with open(filepath, 'w') as FILE:
            FILE.write(base)
        current_hash = md5(filepath)
        if prev_hash != current_hash:
            result['changed'] = True        
    result['message'] += 'Create base in {f_name}\n'.format(f_name=filepath)
    module.exit_json(**result)
     

if __name__ == '__main__':
    main()

    
