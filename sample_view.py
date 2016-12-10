import os, sys, string, smtplib, json, ast
import re
import math
import datetime
import psycopg2
import csv
import cgi, urllib, urlparse
import collections
#authentication utils
from django.contrib.auth.decorators import login_required
# Mail Utils imports
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#Json Utils imports
from django.core.serializers.json import json, DjangoJSONEncoder
#django db utils imports
from django.db import connections
from django.http import HttpResponse
from common_views import *

# Log imports
import logging
import logging.config



ConfFile = ''

ConfigurationsDictionary = ReadandLoadConfigFile.read_and_get_conf_name_value_pairs(ConfFile)

#Get all the details from Conf File
LIMIT_VAL = ConfigurationsDictionary["LIMIT"].strip()
LOG_PATH = ConfigurationsDictionary["LOG_PATH"].strip()
API_TABLE_NAME = ConfigurationsDictionary["API_TABLE_NAME"].strip()
API_SCHEMA_NAME = ConfigurationsDictionary["API_SCHEMA_NAME"].strip()
ACCESS_TOKEN_INVALID_MSG = ConfigurationsDictionary["ACCESS_TOKEN_INVALID_MSG"].strip()
ACCESS_TOKEN_VALID_MSG = ConfigurationsDictionary["ACCESS_TOKEN_VALID_MSG"].strip()
BAD_REQUEST = ConfigurationsDictionary["BAD_REQUEST"].strip()
NO_COUNT = ConfigurationsDictionary["NO_COUNT"].strip()
DATA_NOT_AVAILABLE = ConfigurationsDictionary["DATA_NOT_AVAILABLE"].strip()
MISSING_PARAM_IN_REQUEST = ConfigurationsDictionary["MISSING_PARAM_IN_REQUEST"].strip()
INCORRECT_DATE_FORMAT = ConfigurationsDictionary["INCORRECT_DATE_FORMAT"].strip()
TO_DATE_MISSING = ConfigurationsDictionary["TO_DATE_MISSING"].strip()
TO_DATE_EMPTY = ConfigurationsDictionary["TO_DATE_EMPTY"].strip()
FROM_DATE_MISSING = ConfigurationsDictionary["FROM_DATE_MISSING"].strip()
FROM_DATE_EMPTY = ConfigurationsDictionary["FROM_DATE_EMPTY"].strip()
MOBILE_EVENT_DATE_EXCEED_MSG = ConfigurationsDictionary["MOBILE_EVENT_DATE_EXCEED_MSG"].strip()
CLIENT_MISSING = ConfigurationsDictionary["CLIENT_MISSING"].strip()
VENUE_MISSING = ConfigurationsDictionary["VENUE_MISSING"].strip()
EXPORT_TYPE_MISSING = ConfigurationsDictionary["EXPORT_TYPE_MISSING"].strip()
CLIENT_EMPTY = ConfigurationsDictionary["CLIENT_EMPTY"].strip()
VENUE_EMPTY = ConfigurationsDictionary["VENUE_EMPTY"].strip()
STATUS_FAILURE = ConfigurationsDictionary["STATUS_FAILURE"].strip()
EXPORT_TYPE_EMPTY = ConfigurationsDictionary["EXPORT_TYPE_EMPTY"].strip()
EXPORT_TYPE_DATE_RESTRICTION_MSG = ConfigurationsDictionary["EXPORT_TYPE_DATE_RESTRICTION_MSG"].strip()
DATE_RANGE_ERROR_MSG = ConfigurationsDictionary["DATE_RANGE_ERROR_MSG"].strip()
INVALID_PAGE_NUMBER = ConfigurationsDictionary["INVALID_PAGE_NUMBER"].strip()
DATE_FORMAT = ConfigurationsDictionary["DATE_FORMAT"].strip()
print LIMIT_VAL


#Initiate the Logger
logging.config.fileConfig(LOG_PATH)
log = logging.getLogger('ExportAPI-Process')

@login_required()
def test(request, *args, **kwargs):
    return HttpResponse('Welcome to Export API oauth Test!', status=200)

#to get the query result
def get_query_result(query,my_cur):
  try:
    my_cur.execute(query)
    desc = my_cur.description
    result = [dict(zip([col[0] for col in desc], row)) for row in my_cur.fetchall()]
    return result
  except Exception,e:
    raise e

#get the available client details based on token_id/user_id
def get_clients(token_id):
  try:
    query = "SELECT client_id from "+API_SCHEMA_NAME+".user_client_venue_assignments where user_id = '{0}'".format(token_id)
    cur = connections["dev_postgres"].cursor()
    result = get_query_result(query,cur)
    return result
  except Exception,e:
    raise e

#fetch the available venue details based on token_id/user_id
def get_venues(token_id):
  try:
    query = "SELECT venue_id from "+API_SCHEMA_NAME+".user_client_venue_assignments where user_id= '{0}'".format(token_id)
    cur = connections["dev_postgres"].cursor()
    result = get_query_result(query,cur)
    return result
  except Exception,e:
    raise e

#get the available export_types  based on token_id/user_id
def get_available_exports(token_id):
  try:
    query = "SELECT array_agg(export_type_id) as export_type_id from "+API_SCHEMA_NAME+".export_type_user_assignments where user_id = '{0}'".format(token_id)
    cur = connections["dev_postgres"].cursor()
    result = get_query_result(query,cur)
    return result
  except Exception,e:
    raise e

#Collect input ID's from user input parameters
def get_request_exports_data(request):
  try:
    master_dict = {}
    client = request.GET['client'].strip().lower()
    venue = request.GET['venue'].strip().lower()
    export_type = request.GET['export_type'].strip().lower()
    client_query = "SELECT id from "+API_SCHEMA_NAME+".clients where client_name = '{0}'".format(client)
    venue_query = "SELECT id from "+API_SCHEMA_NAME+".venues where venue_name = '{0}'".format(venue)
    export_query = "SELECT id from "+API_SCHEMA_NAME+".export_types where export_type_name = '{0}'".format(export_type)
    cur = connections["dev_postgres"].cursor()
    client_result = get_query_result(client_query,cur)
    venue_result = get_query_result(venue_query,cur)
    export_result = get_query_result(export_query,cur)
    if client_result:
      master_dict['clients'] = client_result[0]['id']
    if venue_result:
      master_dict['venues'] = venue_result[0]['id']
    if export_result:
      master_dict['available_exports'] = export_result[0]['id']
    return master_dict
  except ValueError:
    raise ValueError

#Role-Based Access control for export types
def access_role_control(request):
  try:
    request_exports_data = {}
    match_status = False 
    match_count = 0
    assigned_data =  {'clients': [], 'venues': [], 'available_exports' : []}
    # regex = re.compile('^HTTP_')
    # d = dict((regex.sub('', header), value) for (header, value) in request.META.items() if header.startswith('HTTP_'))
    # access_token = d.get('ACCESS_TOKEN')
    # query = "SELECT id FROM "+API_SCHEMA_NAME+".access_tokens where token = '{0}'".format(access_token)
    cur = connections["dev_postgres"].cursor()
    # token_id = get_query_result(query,cur)
    token_id = True
    if token_id:
      # token_id =  token_id[0]['id']
      u_query = "SELECT username,role_id,id FROM "+API_SCHEMA_NAME+".users where id='{0}'".format(request.user.id)
      username = get_query_result(u_query,cur)
      if username:
        role_id = username[0]['role_id']
        user_id = username[0]['id']
        username = username[0]['username']
        u2_query = "SELECT role_name FROM "+API_SCHEMA_NAME+".roles where id='{0}'".format(role_id)
        user_role = get_query_result(u2_query,cur)
        if user_role:
          role_name = user_role[0]['role_name']
          if role_name == 'superadmin':
            assigned_data['clients'].append(0)
            assigned_data['venues'].append(0)
            assigned_data['available_exports'].append(0)
          elif role_name == 'client_admin' or role_name == 'venue_admin':
            available_clients = get_clients(user_id)
            available_venues = get_venues(user_id)
            assigned_data['clients'].extend([x['client_id'] for x in available_clients])
            assigned_data['venues'].extend([x['venue_id'] for x in available_venues])
            assigned_data['available_exports'].append(0)
          elif role_name == 'venue_dept_user':
            available_clients = get_clients(user_id)
            available_venues = get_venues(user_id)
            available_exports = get_available_exports(user_id)
            assigned_data['clients'].extend([x['client_id'] for x in available_clients])
            assigned_data['venues'].extend([x['venue_id'] for x in available_venues])
            assigned_data['available_exports'].extend([x['export_type_id'] for x in available_exports])          
          #match user assigned roles with access parameter values and return access status
          match_count = 0
          if request.GET:
            request_exports_data = get_request_exports_data(request)
            assigned_data['available_exports'] = assigned_data['available_exports'][0]
            for x in assigned_data.keys():

              if role_name == 'venue_dept_user':
                if request_exports_data[x] in assigned_data[x]:
                  match_count = match_count +1
                if match_count == len(assigned_data.keys()):
                  match_status = True

              if role_name == 'client_admin':
                if request_exports_data['clients'] in assigned_data['clients']:
                  match_status = True
    
              if role_name == 'venue_admin':
                if request_exports_data['clients'] in assigned_data['clients'] and request_exports_data['venues'] in assigned_data['venues']:
                  match_status = True
          
          return assigned_data,role_name,match_status
      else:
        return False
  except ValueError:
    raise ValueError("access denied")

#validate the access token from the headers
def is_access_token_valid(request):
    try:
        status = True
        message = None
        regex = re.compile('^HTTP_')
        d = dict((regex.sub('', header), value) for (header, value) in request.META.items() if header.startswith('HTTP_'))
        access_token = d.get('ACCESS_TOKEN')
        query = 'SELECT * FROM '+API_SCHEMA_NAME+'."access_tokens" where token=%s' 
        cur = connections["dev_postgres"].cursor()
        cur.execute(query,(access_token,))
        rows=cur.fetchall()
        if len(rows) > 0:
          return True,ACCESS_TOKEN_VALID_MSG
        else:
          return False,ACCESS_TOKEN_INVALID_MSG

    except Exception , e:
        status = False
        message = "Failed : %s"%str(e)
    return {'status':status, 'message':message}

#Validate the datetime format
def validate_datetime_format(date_text):
    try:
        datetime.datetime.strptime(date_text, DATE_FORMAT)
        return True
    except ValueError:
        raise ValueError(INCORRECT_DATE_FORMAT)

def convert(data):
  if isinstance(data, basestring):
    return str(data)
  elif isinstance(data, collections.Mapping):
    return dict(map(convert, data.iteritems()))
  elif isinstance(data, collections.Iterable):
    return type(data)(map(convert, data))
  else:
    return data

def find_date_diff(from_date,to_date):
  try:
    a = datetime.datetime.strptime(from_date, DATE_FORMAT)
    b = datetime.datetime.strptime(to_date, DATE_FORMAT)
    delta = b - a
    delta  = delta.days
    return delta
  except ValueError:
    raise ValueError(INCORRECT_DATE_FORMAT)
  
#end point for list of export
@login_required()
def list_export(request):
    result = []
    data_lenth=''
    response= {'page':{}}
    client = ''
    venue = ''
    export_type = ''
    role_name = ''
    query = ''
    try:

      # is_token_valid,message = is_access_token_valid(request)
      is_token_valid = True
      if is_token_valid == True:
        log.info('Export list - Access token validation success')
        validated_master_data,role_name,status = access_role_control(request)
        export_type_id =  validated_master_data['available_exports'][0]
        try:
          export_type_id = ", ".join(repr(e) for e in export_type_id)
        except:
          export_type_id = export_type_id 

        client_id =  validated_master_data['clients'][0]
        venue_id =  validated_master_data['venues'][0]
                
        # Collect the exportlist for the users
        client_id_get_query = 'select client_name from '+API_SCHEMA_NAME+'.clients where id = {0}'.format(client_id)
        venue_id_get_query = 'select venue_name from '+API_SCHEMA_NAME+'.venues where id = {0}'.format(venue_id)
        export_id_get_query = 'select array_agg(export_type_name) as export_type_name from '+API_SCHEMA_NAME+'.export_types where id in ({0})'.format(export_type_id)        
        cur = connections["dev_postgres"].cursor()
        clientid_result = get_query_result(client_id_get_query,cur)
        venueid_result = get_query_result(venue_id_get_query,cur)
        exportid_result = get_query_result(export_id_get_query,cur)
        if clientid_result:
          client = clientid_result[0]['client_name']
        if venueid_result:
          venue = venueid_result[0]['venue_name']
        if exportid_result:
          export_type = exportid_result[0]['export_type_name']
          export_type = convert(export_type)
          try:
            export_type = ", ".join(repr(e) for e in export_type) 
          except:
            export_type = export_type

        query = 'select client,venue,array_agg(export_type) as export_type from '+API_SCHEMA_NAME+'.'+API_TABLE_NAME+''
        query2 = ' group by client,venue'
        if role_name == 'superadmin':
          query = query
        elif role_name == 'client_admin':
          query +=  " where client in ('{0}')  ".format(client)
        elif role_name == 'venue_admin':
          query +=  " where client in ('{0}') and venue in ('{1}') ".format(client,venue)
        else:
          query +=  " where client in ('{0}') and venue in ( '{1}') and export_type in ({2}) ".format(client,venue,export_type) 

        query +=  query2
        cur = connections["dev_postgres"].cursor()
        result = get_query_result(query,cur)
        json_data = json.dumps(result)
        item_dict = json.loads(json_data)
        data_lenth =  len(item_dict)
        log.info("Export list - Getting available Export list is done")
      else:
        response = {'status':'Request Failure', 'message':message}
        return HttpResponse(json.dumps(response),content_type='application/json',status=401)

    except Exception,e:
      log.exception(e)
      response = {'status':'Request Failure', 'message':str(e)}
      return HttpResponse(json.dumps(response),content_type='application/json',status=500)
    response['data']= list(result)
    response['version']= str(1.0)
    response['page']['size']= 1000
    response['page']['totalElements']= str(data_lenth)
    response['page']['totalPages']= 1
    response['page']['_pagenumber']= 1
    return HttpResponse(json.dumps(response,cls=DjangoJSONEncoder),content_type='application/json',status=200)

#Validate each user parameters
def validate_user_params(request):
    try:
      error_details = {}
      errors = []
      status = []
      params_missing = []
      debug_statuses = []
      response_statuses = []
      response = {}
      debug = True
      request_params = {}
      num_of_days = ''

      if 'debug' in request.GET and boolean(request['GET']):
          debug = True

      params = ['client','venue','export_type', 'from_date', 'to_date']

      if 'client' not in request.GET:
        params_missing.append(CLIENT_MISSING)
      else:
        if request.GET['client'] == "":
          params_missing.append(CLIENT_EMPTY)
        else:
          request_params['client'] = request.GET['client'].strip().lower()
          
      if 'venue' not in request.GET:
        params_missing.append(VENUE_MISSING)
      else:
        if request.GET['venue'] == "":
          params_missing.append(VENUE_EMPTY)
        else:
          request_params['venue'] = request.GET['venue'].strip().lower()
  
      if 'export_type' not in request.GET:
        params_missing.append(EXPORT_TYPE_MISSING)
      else:
        if request.GET['export_type'] == "":
          params_missing.append(EXPORT_TYPE_EMPTY)
        else:
          if request.GET['export_type'] == "mobile events":
            num_of_days = find_date_diff(request.GET['from_date'], request.GET['to_date'])
            if int(num_of_days) > 10:
              params_missing.append(MOBILE_EVENT_DATE_EXCEED_MSG)
          else:
            request_params['export_type'] = request.GET['export_type'].strip().lower()

      if 'from_date' not in request.GET:
        params_missing.append(FROM_DATE_MISSING)
      else:
        if request.GET['from_date'] == "":
          params_missing.append(FROM_DATE_EMPTY)
        else:
          valid_date = validate_datetime_format(request.GET['from_date'])
          if valid_date == True:
            request_params['from_date'] = request.GET['from_date'].strip().lower()
          else:
            params_missing.append(INCORRECT_DATE_FORMAT)


      from_date = datetime.datetime.strptime(request.GET['from_date'], DATE_FORMAT)
      to_date = datetime.datetime.strptime(request.GET['to_date'], DATE_FORMAT)

      if from_date > to_date:
        params_missing.append(DATE_RANGE_ERROR_MSG)

      num_of_days = find_date_diff(request.GET['from_date'], request.GET['to_date'])
      if int(num_of_days) > 365:
        params_missing.append(EXPORT_TYPE_DATE_RESTRICTION_MSG)


      if 'to_date' not in request.GET:
        params_missing.append(TO_DATE_MISSING)
      else:
        if request.GET['to_date'] == "":
          params_missing.append(TO_DATE_EMPTY)
        else:
          valid_date = validate_datetime_format(request.GET['to_date'])
          if valid_date == True:
            request_params['to_date'] = request.GET['to_date'].strip().lower()
          else:
            params_missing.append(INCORRECT_DATE_FORMAT)

      if params_missing:
        params_missing_str = MISSING_PARAM_IN_REQUEST + ',  '.join(params_missing)
        errors.append(params_missing_str)
        if debug:
          debug_statuses.append(params_missing_str)
          response = {'details':errors,'status':'Request Failure', 'reason':'invalid parameters, see the details'}
          status = False
          return response,status
      else:
        response = {'status':'Request success', 'reason':'valid parameters'}
        status = True
        return response,status

    except Exception, e:
      log.exception(e)
      response = {'status':'Request Failure', 'message':str(e)}
      status = False
      return response,status

#Export the details from each source based on user parameters
@login_required()
def get_export(request):
  data_lenth= ''
  final_resultset= []
  per_page = ''
  source_db = ''
  count_query = ''
  database_type = ''
  order_by_field= ''
  page_count = ''
  sql_script_file_path = ''
  role_name = ''
  match_status = ''
  current_page_number = ''
  MAX_LIMIT = int(LIMIT_VAL)
  response = {'links':{},'page':{}}
  is_params_valid={}
  assigned_data = {}
  offset_value = 0
  api_start_time = datetime.datetime.now()
  request_baseurl = request.get_full_path()
  request_baseurl =  request_baseurl.split('&page=')
  request_baseurl =  request_baseurl[0]
  try:
    # is_token_valid,message = is_access_token_valid(request)
    is_params_valid,status = validate_user_params(request)
    #validate the user access token
    is_token_valid = True
    if is_token_valid:
      log.info("Access token validation success")
      #end user input params validation status
      if status == True:
        log.info("User parameters validated")
        client = request.GET['client'].strip().lower()
        venue = request.GET['venue'].strip().lower()
        export_type = request.GET['export_type'].strip().lower()
        from_date = request.GET['from_date'].strip().lower()
        to_date = request.GET['to_date'].strip().lower()

        assigned_data,role_name,match_status = access_role_control(request)

        if match_status == True or role_name == 'superadmin':

          #fetch the sourcedb and query based on user input params
          query = "SELECT exportsourcedb,order_by_field,sql_script_file_path,count_query from "+API_SCHEMA_NAME+"."+API_TABLE_NAME+" where client = '{0}' and venue = '{1}' and export_type= '{2}'".format( client,venue,export_type)
          cur = connections["dev_postgres"].cursor()
          result = get_query_result(query,cur)
          if result:
            source_db = result[0]['exportsourcedb'] 
            sql_script_file_path = result[0]['sql_script_file_path']
            count_query = result[0]['count_query']
            order_by_field = result[0]['order_by_field']
          else:
            log.info("No matching data for given input parameters")
            response = {'status':'Request failure','message':'No matching data found for client: '+client+' , venue: '+venue+', export_type: '+export_type+''}
            return HttpResponse(json.dumps(response),content_type='application/json',status=404) 

          conn = connections[source_db].cursor()
          log.info("Export Source db "+str(source_db)+" connected")
          log.info("Sql script File path for  "+str(source_db)+" is "+str(sql_script_file_path)+" here")
          readfile = open(sql_script_file_path)
          query_string = readfile.read()
          query_string = query_string.strip()
          readfile.close()

          #Adding limit from user input else set default value
          if 'limit' in request.GET:
            if int(request.GET.get('limit')) > MAX_LIMIT:
              limit = MAX_LIMIT
            else:
              limit = request.GET.get('limit').strip()
          else:
            limit = MAX_LIMIT

          if request.GET.get('page'):
            if int(request.GET.get('page')) == 0:
              current_page_number = 1
            else:
              current_page_number = request.GET.get('page')
          else:
            current_page_number = 1

          #set offset value based on the page number
          offset_value = int(current_page_number)-1
          offset_value = offset_value * int(limit)

          #count query replace the date range with the input value date params
          count_query = count_query.replace('$$FROM_DATE$$',from_date)
          count_query = count_query.replace('$$TO_DATE$$',to_date)

          #replace the date range with the input value date params
          query_string = query_string.replace('$$FROM_DATE$$',from_date)
          query_string = query_string.replace('$$TO_DATE$$',to_date)
          # query_string = query_string+' limit '+ str(limit) +' offset '+ str(offset_value)
          try:
            query_split_orderby = query_string.split('order by')
            query_string = query_split_orderby[0]
          except:
            query_string = query_string

          query_string = query_string+' order by '+order_by_field+' desc limit '+ str(limit) +' offset '+ str(offset_value)
          # print query_string
          #count query Run details
          log.info("Count Query Start Time "+str(datetime.datetime.utcnow()))
          res_count = get_query_result(count_query,conn)
          log.info("Count Query End Time "+str(datetime.datetime.utcnow()))

          #fetch the data from source
          log.info("SQL Query: "+query_string)
          log.info("Query Start Time "+str(datetime.datetime.utcnow()))
          res = get_query_result(query_string,conn)
          log.info("Query End Time "+str(datetime.datetime.utcnow()))

          #total length of pages
          json_data = json.dumps(list(res),cls=DjangoJSONEncoder)
          item_dict = json.loads(json_data)
          data_lenth =  len(item_dict)

          if res_count:
            page_count =  int(res_count[0]['count'])
          else:
            log.info("No result count for inputs")
            response = {'status':'Request failure','message':NO_COUNT}
            return HttpResponse(json.dumps(response),content_type='application/json',status=404)

          if page_count <= 0:
            log.info("No result count for inputs")
            response = {'status':'Request failure','message':NO_COUNT}
            return HttpResponse(json.dumps(response),content_type='application/json',status=404)

          overallpage_count =  math.ceil(int(page_count)/float(limit))

          print page_count

          if int(current_page_number) > 1 and int(current_page_number) <= int(overallpage_count):
            response['links']['previous_page'] = urllib.unquote(request_baseurl)+'&page='+str(int(current_page_number) - 1)

          if int(current_page_number) < int(overallpage_count):
            response['links']['next_page'] = urllib.unquote(request_baseurl)+'&page='+str(int(current_page_number) + 1)

          if int(current_page_number) > int(overallpage_count):
            response = {'status':'Request Failure','message':INVALID_PAGE_NUMBER}
            return HttpResponse(json.dumps(response),content_type='application/json',status=404)
          #combine result to one dict
          response['data']= list(res)
          response['version']= str(1.0)
          response['page']['size']= str(limit)
          response['page']['totalElements']= str(data_lenth)
          response['page']['totalPages']= int(overallpage_count)
          response['page']['pagenumber']=str(current_page_number)

          api_end_time = datetime.datetime.now()
          api_processing_time = api_end_time - api_start_time

          log.info("Total Processing time for api call = " +str(api_processing_time))        
          return HttpResponse(json.dumps(response,cls=DjangoJSONEncoder),content_type='application/json',status=200)
        else:
          log.info("Invalid user parameters")
        response = {'status':'Request Failure','message':"Access Denied"}
        return HttpResponse(json.dumps(response),content_type='application/json',status=401)

      else:
        log.info("Invalid user parameters")
        response = {'message':is_params_valid}
        return HttpResponse(json.dumps(response),content_type='application/json',status=400)
    else:
      log.info("Access token is not valid")
      response = {'status':'Request Failure','message':message}
      return HttpResponse(json.dumps(response),content_type='application/json',status=401)

  except Exception, e:
    log.exception(e)
    response = {'status':'Request Failure', 'message':str(e)}
    return HttpResponse(json.dumps(response),content_type='application/json',status=500)

    
