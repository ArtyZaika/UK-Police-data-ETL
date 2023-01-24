# Flask REST application
# Return different aggregated statistics and raw data
#
import os
from io import BytesIO
from flask import Flask, request, send_file
from flask_restx import Resource, Api, fields
import pandas as pd
import logging

app = Flask(__name__)
api = Api(app=app, title='Police data API',
                    version='1.0',
                    description='These are Swagger API descriptions')

app.config['RESTX_MASK_SWAGGER'] = False
app.config["PROPAGATE_EXCEPTIONS"] = False 

DATA_DIR = '/usr/local/datasets'
stats_path = f"{DATA_DIR}/statistics"
merged_data_path = f"{DATA_DIR}/merged_data"

response_success = api.model('SuccessResponse', {
    'success': fields.Boolean(True)    
})

response_error = api.model('ErrorResponse', {
    'success': fields.Boolean(False),
    'message': fields.String('Server Exception'), 
    'errors': fields.String    
})

crime_types_by_district_fields = api.model('ResponseFields', {
    'crimeType': fields.String,
    'districtName': fields.String,
    'cases': fields.Integer,
    'percent': fields.Float
})

crime_types_by_district_arr = api.model('ResponseFieldsArray', {
    'results': fields.List(fields.Nested(crime_types_by_district_fields)),
    'success': fields.Boolean(True)
})

crime_types_fields = api.model('ResponseFields', {
    'crimeType': fields.String,
    'cases': fields.Integer,
    'percent': fields.Float
})

crime_types_arr = api.model('ResponseFieldsArray', {
    'results': fields.List(fields.Nested(crime_types_fields)),
    'success': fields.Boolean(True)
})

outcomes_by_district_fields = api.model('ResponseFields', {
    'lastOutcome': fields.String,
    'districtName': fields.String,
    'cases': fields.Integer,
    'percent': fields.Float
})

outcomes_by_district_arr = api.model('ResponseFieldsArray', {
    'results': fields.List(fields.Nested(outcomes_by_district_fields)),
    'success': fields.Boolean(True)
})

occurences_by_district_fields = api.model('ResponseFields', {    
    'districtName': fields.String,
    'cases': fields.Integer,
    'percent': fields.Float
})

occurences_by_district_arr = api.model('ResponseFieldsArray', {
    'results': fields.List(fields.Nested(occurences_by_district_fields)),
    'success': fields.Boolean(True)
})

outcomes_fields = api.model('ResponseFields', {    
    'lastOutcome': fields.String,
    'cases': fields.Integer,
    'percent': fields.Float
})

outcomes_arr = api.model('ResponseFieldsArray', {
    'results': fields.List(fields.Nested(outcomes_fields)),
    'success': fields.Boolean(True)
})

def _prepare_success_out(df_path: str):
    _stats = pd.read_parquet(f'{stats_path}/{df_path}')
    results = _stats.to_dict(orient='records') 
    return {'success': True, 'results': results}

@api.errorhandler
@api.marshal_with(response_error, code=500)
def default_error_handler(error):
    logging.error(error, exc_info=True)
    return {'success': False, 'message': 'Server Exception', 'errors': f'{str(error)}'}, getattr(error, 'code', 500)

@api.route('/health_check') 
class HealthCheck(Resource):
    @api.doc(description='Health check Endpoint')  
    @api.marshal_with(response_success, code=200, description='Successful response')         
    def get(self):                    
        return {'success': True}

@api.route('/stream_data')
class PoliceData(Resource):
    @api.doc(description='Returns whole aggregated data in Parquet format')
    def get(self):
        bytes_buffer = BytesIO()
        src_df = pd.read_parquet(merged_data_path) 
        src_df.to_parquet(bytes_buffer)
        bytes_buffer.seek(0)
        return send_file(bytes_buffer, attachment_filename='police_data.parquet', as_attachment=True)

@api.route('/crime_types_stats') 
class CrimeTypesStats(Resource):
    @api.doc(description='Statistics by CrimeTypesfor and Desctrics')  
    @api.marshal_with(crime_types_arr, code=200, description='Successful response')         
    def get(self):
        return _prepare_success_out('crime_types_count')

@api.route('/crime_types_by_district_stats') 
class CrimeTypesByDistrictStats(Resource):
    @api.doc(description='Statistics by CrimeTypes and Destrics')  
    @api.marshal_with(crime_types_by_district_arr, code=200, description='Successful response')         
    def get(self):                          
        return _prepare_success_out('crime_types_by_disctrict_count')

@api.route('/outcome_by_disctrict_stats') 
class OutcomesByDistrictStats(Resource):
    @api.doc(description='Statistics by Outcome and Destrics')  
    @api.marshal_with(outcomes_by_district_arr, code=200, description='Successful response')         
    def get(self):                  
        return _prepare_success_out('outcome_by_disctrict_count')

@api.route('/occurences_by_district_stats') 
class OccurencesByDistrictStats(Resource):
    @api.doc(description='Statistics by Occurences in Destrics')  
    @api.marshal_with(occurences_by_district_arr, code=200, description='Successful response')         
    def get(self):          
        return _prepare_success_out('occurences_by_district')

@api.route('/outcomes_stats') 
class OutcomesStats(Resource):
    @api.doc(description='Statistics by Outcomes')  
    @api.marshal_with(outcomes_arr, code=200, description='Successful response')         
    def get(self):                  
        return _prepare_success_out('outcomes_count')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5055, debug=True)