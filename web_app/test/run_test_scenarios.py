from __future__ import annotations
import json, zipfile
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from xml.sax.saxutils import escape
from werkzeug.security import generate_password_hash
import app as app_module

app = app_module.app
app.config['TESTING']=True
BASE=Path(__file__).resolve().parent
USERS=BASE/'users_store.json'
ORIG=USERS.read_text(encoding='utf-8') if USERS.exists() else None


def seed_users():
    store={
      'roles':[],
      'users':[
        {'id':'u_view','username':'viewer_user','password_hash':generate_password_hash('viewer123'),'roles':['viewer'],'is_system':False},
        {'id':'u_oper','username':'operator_user','password_hash':generate_password_hash('operator123'),'roles':['operator'],'is_system':False},
        {'id':'u_conn','username':'conn_user','password_hash':generate_password_hash('conn12345'),'roles':['connection_manager'],'is_system':False},
        {'id':'u_inactive','username':'inactive_user','password_hash':generate_password_hash('inactive123'),'roles':['viewer'],'is_active':False,'is_system':False},
        {'id':'u_dev','username':'dev_user','password_hash':generate_password_hash('dev12345'),'roles':['developer'],'is_system':False},
      ]
    }
    USERS.write_text(json.dumps(store,indent=2),encoding='utf-8')


def restore_users():
    if ORIG is None:
        if USERS.exists(): USERS.unlink()
    else:
        USERS.write_text(ORIG,encoding='utf-8')


def set_sess(client, username, roles):
    with client.session_transaction() as s:
        s['is_authenticated']=True; s['username']=username; s['roles']=roles


def clr_sess(client):
    with client.session_transaction() as s:
        s.clear()


def payload(dag_id='demo_test_dag'):
    return {
      'pipeline':{'dag_id':dag_id,'description':'Demo','schedule_mode':'manual','catchup':False,'retries':1,'retry_delay_minutes':5,'alert_emails':['qa@example.com'],'alert_mode':'both','tags':['qa'],'output_filename':f'{dag_id}.py'},
      'nodes':[{'id':'node_1','type':'n8n','label':'N8N','config':{'workflow_name':'wf_demo','http_method':'GET'}}],
      'edges':[]
    }

@contextmanager
def mocks():
    with (
      patch.object(app_module,'list_connections',return_value=[{'connection_id':'h1','conn_type':'http'},{'connection_id':'w1','conn_type':'wasb'},{'connection_id':'x','conn_type':'smtp'}]),
      patch.object(app_module,'create_connection',side_effect=lambda config,p:{'connection_id':p.get('connection_id','new'),**p}),
      patch.object(app_module,'update_connection',side_effect=lambda config,cid,p:{'connection_id':cid,**p}),
      patch.object(app_module,'delete_connection',return_value=None),
      patch.object(app_module,'trigger_dag_run',return_value={'dag_id':'demo_test_dag','dag_run_id':'run1','state':'queued'}),
      patch.object(app_module,'get_dag_run_status',return_value={'state':'running'}),
      patch.object(app_module,'list_dag_run_task_instances',return_value=[{'task_id':'node_1','state':'success'}]),
      patch.object(app_module,'get_task_instance_log',return_value={'content':'log','try_number':1}),
      patch.object(app_module,'retry_task_instance',return_value={'state':'queued'}),
      patch.object(app_module,'check_dag_readiness',return_value={'ready':True,'dag_id':'demo_test_dag'}),
      patch.object(app_module,'get_airflow_platform_snapshot',return_value={'version':'mock'})
    ):
      yield


def rec(rows, tc, area, scenario, ok, reason=''):
    rows.append({'Test Case ID':tc,'Area':area,'Scenario':scenario,'Status':'Passed' if ok else 'Failed','Failure Reason':'' if ok else reason})


def cn(i):
    s=''
    while i>0:
        i,r=divmod(i-1,26); s=chr(65+r)+s
    return s


def cell(ref,val,style=None):
    t=escape(str(val if val is not None else ''))
    a=f' s="{style}"' if style is not None else ''
    return f'<c r="{ref}" t="inlineStr"{a}><is><t>{t}</t></is></c>'

def write_xlsx(rows, outp):
    headers=['Test Case ID','Area','Scenario','Status','Failure Reason']
    all_rows=[headers]+[[r[h] for h in headers] for r in rows]
    rxml=[]
    for ri,row in enumerate(all_rows,1):
        cells=[]
        for ci,v in enumerate(row,1):
            st=None
            if ci==4 and ri>1: st=1 if v=='Passed' else 2
            cells.append(cell(f"{cn(ci)}{ri}",v,st))
        rxml.append(f'<row r="{ri}">{"".join(cells)}</row>')

    sheet=('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
      '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
      '<sheetViews><sheetView workbookViewId="0"/></sheetViews><sheetFormatPr defaultRowHeight="15"/>'
      '<cols><col min="1" max="1" width="14" customWidth="1"/><col min="2" max="2" width="18" customWidth="1"/><col min="3" max="3" width="56" customWidth="1"/><col min="4" max="4" width="12" customWidth="1"/><col min="5" max="5" width="72" customWidth="1"/></cols>'
      '<sheetData>'+''.join(rxml)+'</sheetData></worksheet>')

    wb='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Test Results" sheetId="1" r:id="rId1"/></sheets></workbook>
'''
    wbrel='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>
'''
    styles='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF92D050"/><bgColor indexed="64"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFF7C80"/><bgColor indexed="64"/></patternFill></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="3"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="0" fillId="1" borderId="0" xfId="0" applyFill="1"/><xf numFmtId="0" fontId="0" fillId="2" borderId="0" xfId="0" applyFill="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>
'''
    rootrel='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>
'''
    ctype='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>
'''
    with zipfile.ZipFile(outp,'w',compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml',ctype); z.writestr('_rels/.rels',rootrel); z.writestr('xl/workbook.xml',wb); z.writestr('xl/_rels/workbook.xml.rels',wbrel); z.writestr('xl/styles.xml',styles); z.writestr('xl/worksheets/sheet1.xml',sheet)


def run():
    seed_users(); rows=[]
    with app.test_client() as c, mocks():
        clr_sess(c); r=c.get('/login'); rec(rows,'AUTH-001','Authentication','Login page is accessible',r.status_code==200,f'HTTP {r.status_code}')
        clr_sess(c); r=c.post('/login',data={'username':'admin','password':'admin123'},follow_redirects=False); rec(rows,'AUTH-002','Authentication','Valid admin login',r.status_code in (302,303),f'HTTP {r.status_code}')
        clr_sess(c); r=c.post('/login',data={'username':'admin','password':'wrong'},follow_redirects=False); rec(rows,'AUTH-003','Authentication','Invalid password rejected',r.status_code==200 and b'Invalid username or password' in r.data,f'HTTP {r.status_code}')
        clr_sess(c); r=c.post('/login',data={'username':'inactive_user','password':'inactive123'},follow_redirects=False); denied=(r.status_code==200 and b'Invalid username or password' in r.data); rec(rows,'AUTH-004','Authentication','Inactive user cannot login',denied,f'Inactive user accepted (HTTP {r.status_code})')
        clr_sess(c); r=c.get('/api/modules'); rec(rows,'AUTH-005','Authentication','Protected API requires auth',r.status_code==401,f'HTTP {r.status_code}')

        set_sess(c,'viewer_user',['viewer']); r=c.post('/api/generate-dag',json=payload('role_viewer_dag')); rec(rows,'ROLE-001','Authorization','Viewer cannot generate DAG',r.status_code==403,f'HTTP {r.status_code}')
        set_sess(c,'operator_user',['operator']); r1=c.post('/api/run-dag',json={'dag_id':'demo_test_dag','conf':{}}); r2=c.post('/api/admin/users',json={'username':'u1','password':'abcdef','roles':['viewer']}); rec(rows,'ROLE-002','Authorization','Operator can run but not manage users',r1.status_code==200 and r2.status_code==403,f'run={r1.status_code},admin={r2.status_code}')

        set_sess(c,'admin',['admin']); r=c.post('/api/admin/users',json={'username':'test_user','password':'test123','roles':['viewer']}); ok=r.status_code==200; uid=(r.get_json() or {}).get('user',{}).get('id') if ok else None; rec(rows,'USER-001','Admin Users','Create user with valid roles',ok,f'HTTP {r.status_code}')
        r=c.post('/api/admin/users',json={'username':'test_user','password':'test123','roles':['viewer']}); rec(rows,'USER-002','Admin Users','Reject duplicate username',r.status_code==409,f'HTTP {r.status_code}')
        if uid:
            r=c.put(f'/api/admin/users/{uid}',json={'username':'test_user','roles':['operator','viewer'],'password':''}); rec(rows,'USER-003','Admin Users','Update user roles',r.status_code==200,f'HTTP {r.status_code}')
            r=c.delete(f'/api/admin/users/{uid}'); rec(rows,'USER-004','Admin Users','Delete user',r.status_code==200,f'HTTP {r.status_code}')
        else:
            rec(rows,'USER-003','Admin Users','Update user roles',False,'Skipped: USER-001 failed'); rec(rows,'USER-004','Admin Users','Delete user',False,'Skipped: USER-001 failed')

        r=c.post('/api/admin/roles',json={'role_name':'qa_role','permissions':['builder_view','pipeline_load']}); rec(rows,'ROLEM-001','Admin Roles','Create custom role',r.status_code==200,f'HTTP {r.status_code}')
        r=c.put('/api/admin/roles/qa_role',json={'role_name':'qa_role','permissions':['builder_view','dag_generate']}); rec(rows,'ROLEM-002','Admin Roles','Update custom role',r.status_code==200,f'HTTP {r.status_code}')
        r=c.delete('/api/admin/roles/qa_role'); rec(rows,'ROLEM-003','Admin Roles','Delete custom role',r.status_code==200,f'HTTP {r.status_code}')

        set_sess(c,'dev_user',['developer']); r=c.get('/api/modules'); rec(rows,'MOD-001','Module Registry','List modules endpoint',r.status_code==200,f'HTTP {r.status_code}')
        p=payload('bad1'); p['pipeline']['dag_id']='123bad-id'; r=c.post('/api/validate-pipeline',json=p); rec(rows,'PIPE-001','Validation','Reject invalid dag_id',r.status_code==200 and (r.get_json() or {}).get('valid') is False,f'HTTP {r.status_code}')
        p=payload('bad2'); p['pipeline']['schedule_mode']='cron'; p['pipeline']['schedule_cron']=''; r=c.post('/api/validate-pipeline',json=p); rec(rows,'PIPE-002','Validation','Cron mode requires expression',r.status_code==200 and (r.get_json() or {}).get('valid') is False,f'HTTP {r.status_code}')

        r=c.post('/api/generate-dag',json=payload('gen_ok')); j=r.get_json() or {}; ok=r.status_code==200 and j.get('success') is True; rec(rows,'DAG-001','DAG Generation','Generate DAG with valid payload',ok,f'HTTP {r.status_code}'); fn=j.get('generated_filename','') if ok else ''
        r=c.post('/api/generate-dag',json={**payload('x'), 'pipeline':{**payload('x')['pipeline'],'dag_id':'1bad'}}); rec(rows,'DAG-002','DAG Generation','Reject invalid DAG generation payload',r.status_code==400,f'HTTP {r.status_code}')
        if fn:
            r=c.get(f'/api/generated-dag/{fn}'); rec(rows,'DAG-003','DAG Generation','Fetch generated DAG',r.status_code==200,f'HTTP {r.status_code}')
        else:
            rec(rows,'DAG-003','DAG Generation','Fetch generated DAG',False,'No file from DAG-001')

        set_sess(c,'operator_user',['operator']); r=c.post('/api/run-dag',json={'dag_id':'demo_test_dag','conf':{}}); rec(rows,'RUN-001','Run','Run DAG',r.status_code==200,f'HTTP {r.status_code}')
        r=c.get('/api/runs/demo_test_dag/run1/tasks/node_1/logs?try_number=abc'); rec(rows,'RUN-002','Run','Reject invalid try_number',r.status_code==400,f'HTTP {r.status_code}')

        set_sess(c,'dev_user',['developer']); r=c.post('/api/save-pipeline',json={'filename':'my_pipeline.json','pipeline_data':payload('persist_case')}); rec(rows,'SAVE-001','Persistence','Save pipeline',r.status_code==200,f'HTTP {r.status_code}')
        r=c.post('/api/load-pipeline',json={'filename':'../bad.json'}); rec(rows,'SAVE-002','Persistence','Reject invalid load filename',r.status_code==400,f'HTTP {r.status_code}')

        set_sess(c,'conn_user',['connection_manager']); r=c.get('/api/airflow/connections'); cons=(r.get_json() or {}).get('connections',[]) if r.status_code==200 else []; types={i.get('conn_type') for i in cons}; rec(rows,'CONN-001','Connections','List only supported types',r.status_code==200 and types.issubset({'http','wasb'}),f'HTTP {r.status_code}; types={sorted(types)}')

        set_sess(c,'admin',['admin']); sf=BASE/'saved_pipelines'/'tmp_delete_case.json'; sf.write_text('{}',encoding='utf-8'); r=c.delete('/api/admin/saved-pipelines/tmp_delete_case.json'); rec(rows,'ADMIN-001','Admin Files','Delete saved pipeline file',r.status_code==200,f'HTTP {r.status_code}')
        gf=BASE/'generated_dags'/'tmp_delete_case.py'; gf.write_text('# tmp',encoding='utf-8'); r=c.delete('/api/admin/generated-dags/tmp_delete_case.py'); rec(rows,'ADMIN-002','Admin Files','Delete generated DAG file',r.status_code==200,f'HTTP {r.status_code}')

        r=c.get('/api/health'); rec(rows,'GEN-001','General','Health endpoint returns ok',r.status_code==200 and (r.get_json() or {}).get('status')=='ok',f'HTTP {r.status_code}')

    restore_users()
    outp=BASE/'application_test_scenarios_tested.xlsx'
    write_xlsx(rows,outp)
    p=sum(1 for x in rows if x['Status']=='Passed'); f=len(rows)-p
    print(f'Created: {outp}'); print(f'Total executed: {len(rows)}'); print(f'Passed: {p}'); print(f'Failed: {f}')
    if f:
        print('Failed cases:')
        for x in rows:
            if x['Status']=='Failed': print(f"- {x['Test Case ID']}: {x['Failure Reason']}")

if __name__=='__main__':
    try:
        run()
    finally:
        restore_users()
