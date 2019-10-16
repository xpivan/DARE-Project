import requests
import json, os
import pdb

#fm
try:
    from IPython.display import display, clear_output
except:
    pass

def login(username, password, hostname):
    data = {'username': username, 'password': password}
    url = hostname + '/api-token-auth/'
    r = requests.post(url, data)
    logged_in = r.status_code == 200
    if logged_in:
        token = json.loads(r.text)['token']
    return token


def get_auth_header(token):
    '''Return the authentication header as used for requests to the registry.'''
    return {'Authorization': 'Token %s' % (token)}

# Get workspace url by name


def get_workspace(name, creds):
    # Get json response
    req = requests.get(creds['D4P_REGISTRY_HOSTNAME'] + '/workspaces/',
                       headers=creds['header'])
    resp_json = json.loads(req.text)
    # Iterate and retrieve
    return ([i['url'] for i in resp_json if i['name'] == name][0],
            [i['id'] for i in resp_json if i['name'] == name][0])

# Create workspace using d4p registry api


def create_workspace(clone, name, desc, creds):
    # Prepare data for posting
    data = {
        "clone_of": clone,
        "name": name,
        "description": desc
    }
    # Request for d4p registry api
    _r = requests.post(creds['D4P_REGISTRY_HOSTNAME'] + '/workspaces/',
                       headers=creds['header'], data=data)

    # Progress check
    if _r.status_code == 201:
        print('Added workspace: ' + name)
        return get_workspace(name, creds)
    else:
        print ('Add workspace resource returns status_code: ' +
               str(_r.status_code))
        return get_workspace(name, creds)

# Create ProcessingElement using d4p registry api


def create_pe(desc, name, conn, pckg, workspace, clone, peimpls, creds):
    assert isinstance(conn, list)
    assert isinstance(peimpls, list)

    # Prepare data for posting
    data = {
        "description": desc,
        "name": name,
        "connections": conn,
        "pckg": pckg,
        "workspace": workspace,
        "clone_of": clone,
        "peimpls": peimpls
    }
    # Request for d4p registry api
    _r = requests.post(
        creds['D4P_REGISTRY_HOSTNAME'] + '/pes/', headers=creds['header'], data=data)
    # Progress check
    if _r.status_code == 201:
        print('Added Processing Element: ' + name)
        return json.loads(_r.text)['url']
    else:
        print ('Add Processing Element resource returns status_code: ' +
               str(_r.status_code))


#Monitoring
def monitor(creds):  
    while True:
        clear_output(wait=True)
        resp = my_pods(token=auth(), creds=creds)
        pod_pretty_print(json.loads(resp))
        if not json.loads(resp):
            break
        time.sleep(1)


# Create ProcessingElement Implementation using d4p registry api

def create_peimpl(desc, code, parent_sig, pckg, name, workspace, clone, creds):
    # Prepare data for posting
    data = {
        "description": desc,
        "code": code,
        "parent_sig": parent_sig,
        "pckg": pckg,
        "name": name,
        "workspace": workspace,
        "clone_of": clone
    }
    # Request for d4p registry api, verify=False is only for demo purposes
    # nginx is too slow on response, open issue
    _r = requests.post(creds['D4P_REGISTRY_HOSTNAME'] + '/peimpls/',
                       headers=creds['header'], data=data, verify=False)
    # Progress check
    if _r.status_code == 201:
        print('Added Processing Element Implementation: ' + name)
        return json.loads(_r.text)['id']
    else:
        print ('Add Processing Element Implementation resource returns \
                status_code: ' + str(_r.status_code))

# Generate user "access token" / Simulate user login


def auth(length=10):
    #  return ''.join(random.choice(string.ascii_lowercase + \
        #  string.ascii_uppercase) for i in range(length))
    return 'Th1s4sY0urT0k3Nn'

# Spawn mpi cluster and run dispel4py workflow


def submit_d4p(impl_id, pckg, workspace_id, pe_name, n_nodes, token, creds,
                reqs=None, **kw):
    # Prepare data for posting
    data = {
        "user": creds['REG_USERNAME'],
        "pwd": creds['REG_PASSWORD'],
        "impl_id": impl_id,
        "pckg": pckg,
        "wrkspce_id": workspace_id,
        "name": pe_name,
        "n_nodes": n_nodes,
        "access_token": token,
        "reqs": reqs if not(reqs is None) else "None"
    }

    print("data in submit_d4p: ",data)
    print(creds['EXEC_API_HOSTNAME'])
    d4p_args = {}
    for k in kw:
        d4p_args[k] = kw.get(k)
    data['d4p_args'] = d4p_args
    # Request for dare api
    _r = requests.post(creds['EXEC_API_HOSTNAME'] + '/run-d4p', data=json.dumps(data))
    # Progress check
    if _r.status_code == 200:
        print(_r.text)
    else:
        print('DARE api resource / d4p-mpi-spec returns status_code: \
                ' + str(_r.status_code))
        print(_r.text)

def upload(token, path, local_path, creds):
    params = (
        ('dataset_name', 'N/A'),
        ('access_token', token),
        ('path', token+'_'+path),
    ) 
    files = {
        'file': (local_path, open(local_path, 'rb')),
    } 

    _r = requests.post(creds['EXEC_API_HOSTNAME'] + '/upload', params=params, files=files)
    return _r.text

def myfiles(token, creds):
    _r = requests.get(creds['EXEC_API_HOSTNAME'] + '/my-files?access_token='+token)
    return _r.text

def _list(path, creds):
    _r = requests.get(creds['EXEC_API_HOSTNAME'] + '/list?path='+path)
    return _r.text

def download(path, creds, local_path):
    os.system('wget '+creds['EXEC_API_HOSTNAME']+'/download?path='+path+' -O '+local_path)
    return 'Dowloading....'

def delete_workspace(name, creds):
    workspace_url, wid  = get_workspace(name, creds)
    _r = requests.delete(creds['D4P_REGISTRY_HOSTNAME'] + '/workspaces/'+str(wid)+'/',
                         headers=creds['header'])
    # Progress check
    if _r.status_code == 204:
        print('Deleted workspace '+name)
    else:
        print('Delete workspace returned status code: '+str(_r.status_code))
        print(_r.text)

# Spawn mpi cluster and run specfem workflow

def submit_specfem(n_nodes, data_url, token, creds):
    # Prepare data for posting
    data = {
            "n_nodes": n_nodes,
            "data_url": data_url,
            "access_token": token
            }
    # Request for dare api
    _r = requests.post(creds['EXEC_API_HOSTNAME'] + '/run-specfem', data=json.dumps(data))
    # Progress check
    if _r.status_code == 200:
        print(_r.text)
    else:
        print('DARE api resource / d4p-mpi-spec returns status_code: \
                '+str(_r.status_code))

def my_pods(token, creds):
    _r = requests.get(creds['EXEC_API_HOSTNAME'] + '/my-pods?access_token='+token)
    return _r.text

def send2drop(token, path, creds):
    _r = requests.get(creds['EXEC_API_HOSTNAME'] + '/send2drop?access_token='+token+'&path='+path)
    return _r.text

def find_upload_path(_json, UPLOAD_PATH):
    print('Uploaded files......')

    uploads_path = []
    exec_path = []

    for i in _json['uploads']:
        if UPLOAD_PATH in i['path']:
            #uploads_path.append(i['path'])
            #exec_path.append(i['exec_path'])
            uploads_path = i['path']
            exec_path = i['exec_path']

    return uploads_path, exec_path

def create_new_upload_path(_json, UPLOAD_PATH):
    upload_path, exec_path = find_upload_path(_json, UPLOAD_PATH)
    len_ULP = len(UPLOAD_PATH)

    if not upload_path:
        upload_name = UPLOAD_PATH
    else:
        upload_path.sort()
        exec_path.sort()
        loc_word = upload_path[-1].find(UPLOAD_PATH)
        loc_exec_word = exec_path[-1].find(UPLOAD_PATH)
        check_nb_path = loc_word+len(UPLOAD_PATH)
        
        if check_nb_path==len(upload_path[-1]):
            upload_name = UPLOAD_PATH+'-0'
            exec_name = exec_path[-1]+'-0'
        else:
            num_upl = int(upload_path[-1][loc_word+len(UPLOAD_PATH)+1::])
            upload_name = UPLOAD_PATH+'-'+str(num_upl+1)
            exec_name = exec_path[-1][0:loc_exec_word+len(UPLOAD_PATH)]+'-'+str(num_upl+1)
        #exec_dir=exec_path[-1][0:loc_exec_word]
    return upload_name, exec_name#, exec_dir

def files_pretty_print(_json):
    print('Uploaded files......')
    print('\n')
    uploads_path = []
    exec_path = []
    for i in _json['uploads']:
        print('API LOCAL path: '+i['path'])
        print('Execution path: '+i['exec_path'])
        uploads_path.append(i['path'])
        exec_path.append(i['exec_path'])

        
        print('\n')

    print('\n')
    print('Files generated from runs......')
    print('\n')
    for i in _json['run']:
        print('Api Local path: '+i['path'])
        print('Execution path: '+i['exec_path'])
        print('\n')

    


def _list_pretty_print(_json):
    print('Listing files......')
    print('\n')
    for i in _json['files']:
        print('Api Local path: '+i['path'].split('/')[-1])
        print('\n')
    print('\n')
    return path

def pod_pretty_print(_json):
    print('Running containers...')
    print('\n')
    for i in _json:
        print('Container name: '+i['name'])
        print('Container status: '+i['status'])
        print('\n')
    print('\n')


# Create ProcessingElement Implementation using d4p registry api

def create_peimpl_temp(desc, code, parent_sig, pckg, name, workspace, clone, creds):
    # Prepare data for posting
    data = {
        "description": desc,
        "code": code,
        "parent_sig": parent_sig,
        "pckg": pckg,
        "name": name,
        "workspace": workspace,
        "clone_of": clone
    }
    # Request for d4p registry api, verify=False is only for demo purposes
    # nginx is too slow on response, open issue
    _r = requests.post('http://83.212.73.39:30604' + '/peimpls/',
                       headers=creds['header'], data=data, verify=False)
    # Progress check
    if _r.status_code == 201:
        print('Added Processing Element Implementation: ' + name)
        return json.loads(_r.text)['id']
    else:
        print ('Add Processing Element Implementation resource returns \
                status_code: ' + str(_r.status_code))