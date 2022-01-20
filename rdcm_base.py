from ansible.module_utils.basic import AnsibleModule
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
import os
import dns.resolver
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

# Get module's output dict and path inventory(InventoryManager class)
# Returns string of the base
def create_base(result, inventory, base_name):
    base = base_template_head.replace('__root__', base_name)
    STAGES = set(['st-BEGIN', 'st-RUN', 'st-CHANGE'])
    resolver = dns.resolver.Resolver()
    resolver.timeout = 5
    result['dns_servers'] = resolver.nameservers
    for org in inventory.groups.keys():
        if not 'org-' in org:
            continue
        org_tmp = group_template
        org_tmp = org_tmp.replace('__group_name__', org.split('-')[1])
        for host in inventory.groups[org].hosts:
            groups = set([x.name for x in host.groups])
            # check host in active stage and in o-win group, if not - skip it
            if not 'o-win' in groups or STAGES.intersection(groups) == set():
                continue
            ip = ''
            if host.vars.get('ansible_host') != None:
                ip = host.vars['ansible_host']
            else:
                if 'a-STAT' in groups:
                    can_resolve = False
                    try_count = 0
                    while not can_resolve and try_count < 2:
                        try:
                            ip = resolver.resolve(host.name)[0].address
                            can_resolve = True
                        except dns.resolver.NXDOMAIN:
                            ip = 'NXDOMAIN'
                            break
                        except dns.resolver.LifetimeTimeout:
                            can_resolve = False
                        finally:
                            try_count += 1
                    if not can_resolve:
                        result['failed_to_resolve'].append(f"Wasn't resolve {host.name}")                            
                elif 'a-DYN' in groups:
                    ip = host.name
            # replace __display_name__ on hostname and __server_name__ on IP
            ser_tmp = server_template.replace(
                "__display_name__</displayName>\n          <name>__server_name__",
                f"{host.name.split('.')[0].lower()}</displayName>\n          <name>{ip}"
                )
            # If host has RDP_PORT attribute add connection settings XML template
            if host.vars.get('RDP_PORT') != None:
                ser_tmp = ser_tmp.replace(
                    '</server>', 
                    f"{connection_set.replace('__port__', str(host.vars['RDP_PORT']))}      </server>"
                    )
            # if host in c-SR group add it in ser_collector
            # if in c-PC - add in pc_collector
            if 'c-SR' in groups:
                ser_collector.append(ser_tmp)
            elif 'c-PC' in groups:
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
    inventory_file_name = inventory_dir
    data_loader = DataLoader()
    inventory = InventoryManager(
        loader = data_loader,
        sources=[inventory_file_name])
    base = create_base(result, inventory, base_name)
    if not os.path.exists(f'{base_dir}/{base_name}.rdg'):
        with open(f'{base_dir}/{base_name}.rdg', 'w') as FILE:
            FILE.write(base)
        result['changed'] = True
    else:
        prev_hash = md5(f'{base_dir}/{base_name}.rdg')
        with open(f'{base_dir}/{base_name}.rdg', 'w') as FILE:
            FILE.write(base)
        current_hash = md5(f'{base_dir}/{base_name}.rdg')
        if prev_hash != current_hash:
            result['changed'] = True        
    result['message'] += f'Create base in {base_dir}/{base_name}.rdg\n'
    module.exit_json(**result)
     

if __name__ == '__main__':
    main()

    