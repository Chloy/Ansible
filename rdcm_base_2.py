from ansible.module_utils.basic import AnsibleModule
from ansible_runner import get_inventory
from dns import resolver
import os
import hashlib

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

import os
from re import match, search
import shlex

def parser(i_dir):
    i_files = os.listdir(i_dir)

    res = {
        'hosts':{},
        'groups':{}
    }

    for i_file in i_files:
        if 'ORG' in i_file:
            print()
        with open(f'{i_dir}{i_file}') as F:
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
                    res['groups'][g_name]['children'].append(lines[i].strip('\n\r'))
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
                        res['hosts'][attrs[0]] = {}
                    if attrs[0] == 'tm-v-pc-d3w10.es.efsystem.ru':
                        print()
                    if len(attrs) > 1:
                        for k in range(1, len(attrs)):
                            if res['hosts'][attrs[0]].get('vars') == None:
                                res['hosts'][attrs[0]] = {
                                    'vars': {}
                                }
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
    STAGES = set(['st-BEGIN', 'st-RUN', 'st-CHANGE'])
    
    res = resolver.Resolver()
    res.timeout = 5
    result['dns_servers'] = res.nameservers
    for org in inventory['org']['children']:
        if inventory.get(org) == None:
            continue
        org_tmp = group_template
        org_tmp = org_tmp.replace('__group_name__', org.split('-')[1])    
        for host in inventory[org]['hosts']:
            try:
                if (not host in inventory['o-win']['hosts'] or
                    host in inventory['st-END']['hosts'] or 
                    host in inventory['st-INIT']['hosts'] or 
                    host in inventory['st-NONE']['hosts'] or
                    host in inventory['st-TEST']['hosts']):
                    continue
            except KeyError:
                pass
        
            if (inventory['_meta']['hostvars'].get(host) != None and
                inventory['_meta']['hostvars'][host].get('ansible_host') != None):
                ip = inventory['_meta']['hostvars'][host]['ansible_host']
            else:
                if host in inventory['a-STAT']['hosts']:
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
                        result['failed_to_resolve'].append(f"Wasn't resolve {host}")                            
                elif host in inventory['a-DYN']['hosts']:
                    ip = host

            ser_tmp = server_template.replace(
                "__display_name__</displayName>\n          <name>__server_name__",
                f"{host.split('.')[0].lower()}</displayName>\n          <name>{ip}"
                )
            # If host has RDP_PORT attribute add connection settings XML template
            if (inventory['_meta']['hostvars'].get(host) != None and
                inventory['_meta']['hostvars'][host].get('RDP_PORT') != None):
                ser_tmp = ser_tmp.replace(
                    '</server>', 
                    f"{connection_set.replace('__port__', str(inventory['_meta']['hostvars'][host]['RDP_PORT']))}      </server>"
                    )
            # if host in c-SR group add it in ser_collector
            # if in c-PC - add in pc_collector
            if host in inventory['c-SR']['hosts']:
                ser_collector.append(ser_tmp)
            elif host in inventory['c-PC']['hosts']:
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
    # inventory = get_inventory(
    #     action='list', 
    #     inventories=[inventory_dir],
    #     quiet=True,
    #     response_format='json',
    #     export=True
    #     )[0]

    inventory = parser(inventory_dir)
    base = create_base(result, inventory, base_name)
    if not os.path.exists(f'{base_dir}{base_name}.rdg'):
        with open(f'{base_dir}{base_name}.rdg', 'w') as FILE:
            FILE.write(base)
        result['changed'] = True
    else:
        prev_hash = md5(f'{base_dir}{base_name}.rdg')
        with open(f'{base_dir}{base_name}.rdg', 'w') as FILE:
            FILE.write(base)
        current_hash = md5(f'{base_dir}{base_name}.rdg')
        if prev_hash != current_hash:
            result['changed'] = True        
    result['message'] += f'Create base in {base_dir}{base_name}.rdg\n'
    module.exit_json(**result)
     

if __name__ == '__main__':
    main()

    
