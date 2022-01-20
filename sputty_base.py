from ansible.module_utils.basic import AnsibleModule
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
import os
import dns.resolver
import hashlib

# Head of the base
base_template_head = """<?xml version="1.0" encoding="utf-8"?>
<ArrayOfSessionData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
"""
# Tail of the base
base_template_tail = '\n</ArrayOfSessionData>'
# Template with _keywords_ for futher replacement
host_template = """  <SessionData SessionId="__org__/__type__/__hostname__" SessionName="__hostname_short__" ImageKey="__image__" Host="__ip__" Port="__port__" Proto="__proto__" PuttySession="__settings__" Username="__username__" ExtraArgs="__args__" />"""
# InventoryManager does not read groups attributes, where defined SPUTTY_IMG
# So here copy of there values according to device type group
sputty_img = {
  'c-FW': 'drive_network',
  'c-PC': 'computer',
  'c-RT': 'drive_network',
  'c-SR': 'computer',
  'c-SW': 'drive_network'  
}
# Def values for hosts
def_port = '22'
def_proto = 'SSH'
def_session = 'Default Settings'
def_username = ''
def_args = ''


# Calculate file's hash
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.digest()

# Get module's output dict and path inventory(InventoryManager class)
# Returns string of the base
def create_base(result, inventory):
    base = base_template_head
    STAGES = set(['st-BEGIN', 'st-RUN', 'st-CHANGE'])
    TYPES = set(['c-SR', 'c-PC', 'c-RT', 'c-SW', 'c-FW'])
    resolver = dns.resolver.Resolver()
    resolver.timeout = 5
    result['dns_servers'] = resolver.nameservers
    for org in inventory.groups.keys():
        if not 'org-' in org:
            continue
        for host in inventory.groups[org].hosts:
            groups = set([x.name for x in host.groups])
            # check host in active stage, if not - skip it
            if STAGES.intersection(groups) == set():
                continue
            type = TYPES.intersection(groups)
            type = type.pop()
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
            
            port = host.vars.get('SPUTTY_PORT', def_port)
            proto = host.vars.get('SPUTTY_PROTO', def_proto)
            session = host.vars.get('SPUTTY_SESSION', def_session)
            username = host.vars.get('SPUTTY_LOGIN', def_username)
            args = host.vars.get('SPUTTY_ARGUMENTS', def_args)
            try:
                base += f"""  <SessionData SessionId="{org.split('-')[1]}/{type.split('-')[1]}/{host.name}" SessionName="{host.name.split('.')[0].lower()}" ImageKey="{sputty_img[type]}" Host="{ip}" Port="{port}" Proto="{proto}" PuttySession="{session}" Username="{username}" ExtraArgs="{args}" />\n"""
            except KeyError:
                result['failed_to_resolve'].append(f"Key error for {host.name}")                            
    base += base_template_tail
    return base


def main():
    # Define module args and check mode support
    module = AnsibleModule(
        argument_spec=dict(
            inventory_dir=dict(required=True),
            base_dir=dict(required=True)
        ),
        supports_check_mode=True
    )
    # Define module's output
    result = dict(
        changed=False,
        message='',
        failed_to_resolve=[],
        dns_servers = []
    )
    # Exit if module run in check mode
    if module.check_mode:
        module.exit_json(**result)
    
    inventory_dir = module.params['inventory_dir']
    base_dir = module.params['base_dir']
    inventory_file_name = inventory_dir
    data_loader = DataLoader()
    inventory = InventoryManager(
        loader = data_loader,
        sources=[inventory_file_name]
        )
    base = create_base(result, inventory)
    if not os.path.exists(base_dir) or not os.path.exists(f'{base_dir}/v1'):
        os.makedirs(f'{base_dir}/v1')
        result['changed'] = True
    if not os.path.exists(f'{base_dir}/v1/Sessions.XML'):
        with open(f'{base_dir}/v1/Sessions.XML', 'w') as FILE:
            FILE.write(base)
        result['changed'] = True
    else:
        prev_hash = md5(f'{base_dir}/v1/Sessions.XML')
        with open(f'{base_dir}/v1/Sessions.XML', 'w') as FILE:
            FILE.write(base)
        current_hash = md5(f'{base_dir}/v1/Sessions.XML')
        if prev_hash != current_hash:
            result['changed'] = True    
    result['message'] += f'Create base in {base_dir}/v1/Sessions.XML\n'
    module.exit_json(**result)
     

if __name__ == '__main__':
    main()