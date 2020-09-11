"""writer: Sayan"""

import threading
from multiprocessing import Process
import csv, datetime, sys, urllib2, json, os
from urllib2 import URLError,HTTPError
import httplib, pinyin, unidecode
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)

class rovi_delta_verification_lib:

    retry_count = 0

    def init(self):
        self.px_long_title = ''
        self.px_original_title = ''
        self.px_episode_title = ''
        self.px_medium_title = ''
        self.px_run_time = 0
        self.px_release_year = 0
        self.px_record_language = ''
        self.iso_3_char_language = ''
        self.is_group_language_primary = ''
        self.px_aliases = []
        self.px_response = 'Null'
        self.px_season_number = 0
        self.px_episode_number = 0
        self.px_show_id = 0
        self.px_season_program_id = 0
        self.px_category = ''
        self.px_sports_subtitle = ''
        self.px_audio_level = ''
        self.px_movie_type = ''
        self.px_program_color_type = ''
        self.px_variant_parent_id = ''
        self.px_title_parent_id = ''
        self.px_original_episode_title = ''

    def default_params(self):
        self.long_title_match = ''    
        self.medium_title_match = ''
        self.original_title_match = ''
        self.episode_title_match = ''
        self.sport_subtitle_match = ''
        self.run_time_match = ''
        self.release_year_match = ''
        self.audio_level_match = ''
        self.movie_type_match = ''
        self.category_match = ''
        self.program_color_type_match = ''
        self.original_episode_title_match = ''

    def projectx_aliases(self,data_aliases,source):
        #import pdb;pdb.set_trace()
        px_aliases=[]
        for aliases in data_aliases:
            if aliases.get("type")=='alias' and aliases.get("source_name")==source:
                px_aliases.append(unidecode.unidecode(pinyin.get(aliases.get("alias"))))
        return px_aliases    

    def get_mappings_sources(self,projectx_id,mapping_api,token):
        try:
            response_array = []
            mapping_response = lib_common_modules().fetch_response_for_api_(mapping_api,token)
            if mapping_response:
                for response in mapping_response:
                    response_array.append([response['data_source'] + " => " + response["source_id"]])
                return response_array    
            else:
                return
        except (Exception,URLError,HTTPError) as error:
            raise (error,error.reason,mapping_api,token)
            self.retry_count += 1
            if self.retry_count <= 10:
                self.get_mappings_sources(projectx_id,mapping_api,token)
            else:
                self.retry_count = 0    

    def check_mapping_px_id(self,mapping_from_source_api,token):
        try:
            mapping_from_source_api_response = lib_common_modules().fetch_response_for_api_(mapping_from_source_api,token)
            if mapping_from_source_api_response:
                return mapping_from_source_api_response[0]["projectx_id"]
            else:
                return "NA"
        except (Exception,URLError,HTTPError) as error:
            raise (error,error.reason,mapping_from_source_api,token)
            self.retry_count += 1
            if self.retry_count <= 10:
                self.check_mapping_px_id(source_id,mapping_from_source_api,token)
            else:
                self.retry_count = 0

    #TODO: to get meta details of projectx ids
    def getting_projectx_details(self,projectx_id,show_type,source,projectx_programs_api,token):
        #import pdb;pdb.set_trace()
        self.init()
        try:
            projectx_api=projectx_programs_api
            data_px_resp=lib_common_modules().fetch_response_for_api_(projectx_api,token)
            if data_px_resp!=[]:
                for data in data_px_resp:
                    if data.get("long_title") is not None and data.get("long_title") != "":
                        self.px_long_title = unidecode.unidecode(pinyin.get(data.get("long_title")))
                    if data.get("original_title") is not None and data.get("original_title") != "":
                        self.px_original_title = unidecode.unidecode(pinyin.get(data.get("original_title")))
                    if data.get("medium_title") is not None and data.get("medium_title") != "":
                        self.px_medium_title = unidecode.unidecode(pinyin.get(data.get("medium_title")))    
                    if data.get("original_episode_title") != "":
                        self.px_original_episode_title = unidecode.unidecode(pinyin.get(data.get("original_episode_title")))
                    if data.get("episode_title") != "":
                        self.px_episode_title = unidecode.unidecode(pinyin.get(data.get("episode_title")))
                    self.is_group_language_primary = data.get("is_group_language_primary")
                    self.px_record_language = data.get("record_language")
                    self.iso_3_char_language = data.get("iso_3_char_language")
                    self.px_release_year = data.get("release_year")
                    self.px_run_time = data.get("run_time")
                    self.px_show_id = data.get("series_id")
                    self.px_season_program_id = data.get("season_program_id")
                    self.px_category = data.get("category")
                    self.px_sports_subtitle = data.get("sports_subtitle")
                    self.px_audio_level = data.get("audio_level")
                    self.px_movie_type = data.get("movie_type")
                    self.px_program_color_type = data.get("color_type")
                    self.px_variant_parent_id = data.get("variant_parent_id")
                    self.px_title_parent_id = data.get("title_parent_id")

                    if data.get("aliases"):
                        self.px_aliases = self.projectx_aliases(data.get("aliases"),source)
                    try:
                        self.px_season_number = data.get("episode_season_number")
                    except Exception:
                        pass     
                    try:
                        self.px_episode_number = data.get("episode_season_sequence")
                    except Exception:
                        pass

                    return {"px_long_title":self.px_long_title,"px_episode_title":self.px_episode_title,"px_original_episode_title":self.px_original_episode_title,"px_original_title":self.px_original_title,"px_medium_title":self.px_medium_title,"px_aliases":self.px_aliases,"px_release_year":self.px_release_year,"px_run_time":self.px_run_time,"px_season_number":self.px_season_number,"px_episode_number":self.px_episode_number,"is_group_language_primary":self.is_group_language_primary,"record_language":self.px_record_language,"iso_3_char_language":self.iso_3_char_language,"px_series_id":self.px_show_id,"px_season_program_id":self.px_season_program_id,"px_category":self.px_category,"px_sports_subtitle":self.px_sports_subtitle,"px_audio_level":self.px_audio_level,"px_movie_type":self.px_movie_type,"px_program_color_type":self.px_program_color_type,"px_variant_parent_id":self.px_variant_parent_id,"px_title_parent_id":self.px_title_parent_id}
            else:
                return self.px_response

        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,urllib2.URLError,RuntimeError) as error:        
            self.retry_count += 1
            raise ("exception caught getting_projectx_details func..................",type(error),error,projectx_id,show_type,source)
            if self.retry_count <= 10:
                self.getting_projectx_details(projectx_id,show_type,source,projectx_programs_api,token)
            else:
                self.retry_count=0       

    def long_title_validation(self,rovi_long_title,px_long_title):
        self.default_params()
        if rovi_long_title == px_long_title:
            self.long_title_match = "True"
        else:
            self.long_title_match = "False"
        return self.long_title_match

    def medium_title_validation(self,rovi_medium_title,px_medium_title):
        self.default_params()
        if rovi_medium_title == px_medium_title:
            self.medium_title_match = "True"
        else:
            self.medium_title_match = "False"
        return self.medium_title_match        

    def original_title_validation(self,rovi_original_title,px_original_title):
        self.default_params()
        if rovi_original_title == px_original_title:
            self.original_title_match = "True"
        else:
            self.original_title_match = "False"
        return self.original_title_match       

    def original_episode_title_validation(self,rovi_original_episode_title,px_original_episode_title):
        self.default_params()
        if rovi_original_episode_title == px_original_episode_title:
            self.original_episode_title_match = "True"
        else:
            self.original_episode_title_match = "False"
        return self.original_episode_title_match 

    def episode_title_validation(self,rovi_episode_title,px_episode_title):
        #import pdb;pdb.set_trace()
        self.default_params()
        if rovi_episode_title == px_episode_title:
            self.episode_title_match = "True"
        else:
            self.episode_title_match = "False"
        return self.episode_title_match            

    def category_validation(self,rovi_category,px_category):
        self.default_params()
        if rovi_category == px_category:
            self.category_match = "True"
        else:
            self.category_match = "False"
        return self.category_match        

    def sport_subtitle_validation(self,rovi_sport_subtitle,px_sports_subtitle):
        self.default_params()
        if rovi_sport_subtitle == px_sports_subtitle:
            self.sport_subtitle_match = "True"
        else:
            self.sport_subtitle_match = "False"
        return self.sport_subtitle_match      

    def run_time_validation(self,rovi_run_time,px_run_time):
        self.default_params()
        if rovi_run_time == str(px_run_time):
            self.run_time_match = "True"
        else:
            self.run_time_match = "False"
        return self.run_time_match    

    def release_year_validation(self,rovi_release_year,px_release_year):
        self.default_params()
        if rovi_release_year == str(px_release_year):
            self.release_year_match = "True"
        elif rovi_release_year == '' and str(px_release_year) != '':
            self.release_year_match = "Rovi is not providing Release year, but Program has R_Y"    
        else:
            self.release_year_match = "False"
        return self.release_year_match  

    def audio_level_validation(self,rovi_audio_lavel,px_audio_level):
        self.default_params()
        if rovi_audio_lavel == px_audio_level:
            self.audio_level_match = "True"
        else:
            self.audio_level_match = "False"
        return self.audio_level_match

    def movie_type_validation(self,rovi_movie_type,px_movie_type):
        self.default_params()
        if rovi_movie_type == px_movie_type:
            self.movie_type_match = "True"
        else:
            self.movie_type_match = "False"
        return self.movie_type_match 

    def program_color_type_validation(self,rovi_program_color_type,px_program_color_type):
        self.default_params()
        if rovi_program_color_type == px_program_color_type:
            self.program_color_type_match = "True"
        else:
            self.program_color_type_match = "False"
        return self.program_color_type_match   
                

class rovi_delta_verification:

    total = 0

    def __init__(self):
        self.source = "Rovi"
        self.token = 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.fieldnames = ["Show_type","Rovi_id","Rovi_series_id","rovi_season_program_id","rovi_varient_parent_id","rovi_title_parent_id","rovi_is_group_primary_language","rovi_aliases","rovi_record_language","rovi_audio_lavel","rovi_movie_type","rovi_program_color_type","rovi_iso3_char_language","projectx_id","long_title_validation_result","medium_title_validation_result","original_title_validation_result","original_episode_title_validation_result","episode_title_validation_result","category_validation_result","sport_subtitle_validation_result","run_time_validation_result","release_year_validation_result","audio_label_validation_result","movie_type_validation_result","program_color_type_validation_result","px_aliases","px_season_number","px_episode_number","is_group_language_primary","record_language","iso_3_char_language","px_series_id","px_season_program_id","px_variant_parent_id","px_title_parent_id"]

    def get_env_url(self):
        self.projectx_domain = "test.caavo.com"
        self.projectx_mapping_domain_beta = "beta-projectx-api-1289873303.us-east-1.elb.amazonaws.com"
        self.projectx_mapping_domain_preprod = "preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com"
        self.expired_api = 'https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api = "http://%s/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api = 'https://%s/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api = 'http://%s/projectx/%s/mapping/'      

    def get_data_from_sheet(self,input_data,id_):
        self.show_type = str(input_data[id_][0]) 
        self.rovi_id = str(input_data[id_][1])
        self.rovi_series_id = str(input_data[id_][2])
        self.rovi_season_program_id = str(input_data[id_][3])
        self.rovi_varient_parent_id = str(input_data[id_][4])
        self.rovi_title_parent_id = str(input_data[id_][5])
        self.rovi_is_group_primary_language = str(input_data[id_][7])
        self.rovi_long_title = unidecode.unidecode(pinyin.get(input_data[id_][9]))
        self.rovi_medium_title = unidecode.unidecode(pinyin.get(input_data[id_][10]))
        self.rovi_aliases = unidecode.unidecode(pinyin.get(input_data[id_][14]))
        self.rovi_original_title = unidecode.unidecode(pinyin.get(input_data[id_][18]))
        self.rovi_original_episode_title = unidecode.unidecode(pinyin.get(input_data[id_][19]))
        self.rovi_category = str(input_data[id_][20])
        self.rovi_sport_subtitle = str(input_data[id_][21])
        self.rovi_episode_title = unidecode.unidecode(pinyin.get(input_data[id_][22]))
        self.rovi_run_time = str(input_data[id_][24])
        self.rovi_release_year = str(input_data[id_][25])
        self.rovi_record_language = str(input_data[id_][26])
        self.rovi_audio_lavel = str(input_data[id_][30])
        self.rovi_movie_type = str(input_data[id_][32])
        self.rovi_program_color_type = str(input_data[id_][33])
        self.rovi_iso3_char_language = str(input_data[id_][40])
        self.logger.debug ("\n")
        self.logger.debug({"show_type":self.show_type,"Rovi_id":self.rovi_id,"rovi_series_id":self.rovi_series_id})

    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        input_file = "/input/PX_3.0_Rovi_delta_verification_Beta - Main_09092020"
        input_data = lib_common_modules().read_csv(input_file)
        self.logger=lib_common_modules().create_log(os.getcwd()+"/logs/log.txt")
        result_sheet = '/output/Rovi_delta_verification_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file = lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id):
                self.get_data_from_sheet(input_data,_id)
                self.total += 1
                self.logger.debug ("\n")
                self.logger.debug ({"Total tested ": self.total,"thread_name":thread_name})
                projectx_id = rovi_delta_verification_lib().check_mapping_px_id(self.source_mapping_api%(self.projectx_mapping_domain_beta,self.rovi_id,self.source,self.show_type),self.token)
                if projectx_id != "NA":
                    mapping_response = rovi_delta_verification_lib().get_mappings_sources(projectx_id,self.projectx_mapping_api%(self.projectx_mapping_domain_beta,str(projectx_id)),self.token)
                    projectx_details = rovi_delta_verification_lib().getting_projectx_details(projectx_id,self.show_type,self.source,self.projectx_programs_api%(self.projectx_domain,projectx_id),self.token)
                    if projectx_details != "Null":
                        self.logger.debug ("\n")
                        self.logger.debug ({"Projectx_details":projectx_details,"Projectx_id":projectx_id})
                        long_title_validation_result = rovi_delta_verification_lib().long_title_validation(self.rovi_long_title,projectx_details["px_long_title"])
                        medium_title_validation_result = rovi_delta_verification_lib().medium_title_validation(self.rovi_medium_title,projectx_details["px_medium_title"])
                        original_title_validation_result = rovi_delta_verification_lib().original_title_validation(self.rovi_original_title,projectx_details["px_original_title"])
                        original_episode_title_validation_result = rovi_delta_verification_lib().original_episode_title_validation(self.rovi_original_episode_title,projectx_details["px_original_episode_title"])
                        episode_title_validation_result = rovi_delta_verification_lib().episode_title_validation(self.rovi_episode_title,projectx_details["px_episode_title"])
                        category_validation_result = rovi_delta_verification_lib().category_validation(self.rovi_category,projectx_details["px_category"])
                        sport_subtitle_validation_result = rovi_delta_verification_lib().sport_subtitle_validation(self.rovi_sport_subtitle,projectx_details["px_sports_subtitle"])
                        run_time_validation_result = rovi_delta_verification_lib().run_time_validation(self.rovi_run_time,projectx_details["px_run_time"])
                        release_year_validation_result = rovi_delta_verification_lib().release_year_validation(self.rovi_release_year,projectx_details["px_release_year"])
                        audio_label_validation_result = rovi_delta_verification_lib().audio_level_validation(self.rovi_audio_lavel,projectx_details["px_audio_level"])
                        movie_type_validation_result = rovi_delta_verification_lib().movie_type_validation(self.rovi_movie_type,projectx_details["px_movie_type"])
                        program_color_type_validation_result = rovi_delta_verification_lib().program_color_type_validation(self.rovi_program_color_type,projectx_details["px_program_color_type"])
                        
                        self.writer.writerow([self.show_type,self.rovi_id,self.rovi_series_id,self.rovi_season_program_id,self.rovi_varient_parent_id,self.rovi_title_parent_id,self.rovi_is_group_primary_language,self.rovi_aliases,self.rovi_record_language,self.rovi_audio_lavel,self.rovi_movie_type,self.rovi_program_color_type,self.rovi_iso3_char_language,projectx_id,long_title_validation_result,medium_title_validation_result,original_title_validation_result,original_episode_title_validation_result,episode_title_validation_result,category_validation_result,sport_subtitle_validation_result,run_time_validation_result,release_year_validation_result,audio_label_validation_result,movie_type_validation_result,program_color_type_validation_result,projectx_details["px_aliases"],projectx_details["px_season_number"],projectx_details["px_episode_number"],projectx_details["is_group_language_primary"],projectx_details["record_language"],projectx_details["iso_3_char_language"],projectx_details["px_series_id"],projectx_details["px_season_program_id"],projectx_details["px_variant_parent_id"],projectx_details["px_title_parent_id"]])
                    else:
                        self.writer.writerow([self.show_type,self.rovi_id,self.rovi_series_id,self.rovi_season_program_id,self.rovi_varient_parent_id,self.rovi_title_parent_id,self.rovi_is_group_primary_language,self.rovi_aliases,self.rovi_record_language,self.rovi_audio_lavel,self.rovi_movie_type,self.rovi_program_color_type,self.rovi_iso3_char_language,projectx_id,projectx_details])
                else:
                    self.writer.writerow([self.show_type,self.rovi_id,self.rovi_series_id,self.rovi_season_program_id,self.rovi_varient_parent_id,self.rovi_title_parent_id,self.rovi_is_group_primary_language,self.rovi_aliases,self.rovi_record_language,self.rovi_audio_lavel,self.rovi_movie_type,self.rovi_program_color_type,self.rovi_iso3_char_language,projectx_id])

        self.logger.debug ({"Total tested ": self.total})

    # TODO: multi process Operations 
    def thread_pool(self): 
        t1=threading.Thread(target=self.main,args=(1,"thread-1",2000))
        t1.start()
        t2=threading.Thread(target=self.main,args=(2000,"thread-2",4000))
        t2.start()
        t3=threading.Thread(target=self.main,args=(4000,"thread-3",5666))
        t3.start() 

if __name__=="__main__":
    rovi_delta_verification().thread_pool()   