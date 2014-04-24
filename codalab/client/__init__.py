'''
Module codalab.client defines the interface to the bundle system and provides
two concrete implementations: LocalBundleClient for interacting with a local
(in-memory) service and RemoteBundleClient for interacting with an external
service.
'''

def get_address_host(address):
    '''
    Returns the host part of the client's address.
    A client's address is of the form: [<user>@]<host>.
    '''
    tokens = address.split('@')
    if len(tokens) == 1:
        return tokens[0]
    if len(tokens) == 2:
        return tokens[1]
    else:
        raise ValueError("Invalid address: %s." % (address))

def is_local_address(address):
    '''
    Indicates whether the address points to a local (in-memory) service.
    '''
    return get_address_host(address) == 'local'
