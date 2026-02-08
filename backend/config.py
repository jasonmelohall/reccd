#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    db_host: str = "reccd.cptgeqlvdmig.us-west-2.rds.amazonaws.com"
    db_user: str = "reccd_admin"
    db_password: str
    db_name: str = "reccd"
    
    # API Keys
    rainforest_api_key: str
    keepa_api_key: str
    amazon_associate_tag: str = "reccd-20"
    openai_api_key: str | None = None
    
    # App
    user_id: int = 1
    user_email: str = "jasonmelohall@gmail.com"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings():
    return Settings()



