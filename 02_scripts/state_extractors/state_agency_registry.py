#!/usr/bin/env python3
"""
Comprehensive registry of all 50 states' licensing agencies for addiction treatment,
behavioral health, and mental health facilities
"""

STATE_AGENCIES = {
    "AL": {
        "name": "Alabama Department of Mental Health",
        "abbreviation": "ADMH",
        "division": "Division of Mental Health and Substance Abuse Services",
        "website": "https://mh.alabama.gov/",
        "data_portal": "https://mh.alabama.gov/division-of-mental-health-substance-abuse-services/",
        "data_access": "web_portal",
        "licenses": ["SUD Treatment", "Mental Health", "Residential Treatment"],
        "contact": {
            "phone": "(334) 242-3454",
            "email": "Alabama.DMH@mh.alabama.gov"
        }
    },
    
    "AK": {
        "name": "Alaska Division of Behavioral Health",
        "abbreviation": "DBH",
        "division": "Department of Health and Social Services",
        "website": "https://dhss.alaska.gov/dbh/",
        "data_portal": "https://dhss.alaska.gov/dbh/Pages/Prevention/programs/substanceabuse/default.aspx",
        "data_access": "pdf_list",
        "licenses": ["Behavioral Health", "SUD Treatment"],
        "contact": {
            "phone": "(907) 465-3030",
            "email": "dhss.dbh@alaska.gov"
        }
    },
    
    "AZ": {
        "name": "Arizona Department of Health Services",
        "abbreviation": "ADHS",
        "division": "Division of Licensing Services",
        "website": "https://www.azdhs.gov/",
        "data_portal": "https://www.azdhs.gov/licensing/index.php",
        "data_access": "searchable_database",
        "licenses": ["Behavioral Health", "Residential", "Outpatient", "DUI"],
        "api_available": True,
        "contact": {
            "phone": "(602) 364-2536",
            "email": "DLSLicensing@azdhs.gov"
        }
    },
    
    "AR": {
        "name": "Arkansas Division of Aging, Adult and Behavioral Health Services",
        "abbreviation": "DAABHS", 
        "division": "Department of Human Services",
        "website": "https://humanservices.arkansas.gov/about-dhs/daabhs/",
        "data_portal": "https://humanservices.arkansas.gov/about-dhs/daabhs/substance-abuse-prevention-treatment/",
        "data_access": "provider_list",
        "licenses": ["SUD Treatment", "Mental Health"],
        "contact": {
            "phone": "(501) 686-9164"
        }
    },
    
    "CA": {
        "name": "California Department of Health Care Services",
        "abbreviation": "DHCS",
        "division": "Substance Use Disorder Compliance Division",
        "website": "https://www.dhcs.ca.gov/",
        "data_portal": "https://www.dhcs.ca.gov/individuals/Pages/sud-licensed-certified.aspx",
        "data_access": "excel_download",
        "licenses": ["Residential", "Outpatient", "NTP", "Detox", "Recovery Residence"],
        "narcotic_treatment": "https://www.dhcs.ca.gov/services/Pages/NTP.aspx",
        "contact": {
            "phone": "(916) 322-2911"
        }
    },
    
    "CO": {
        "name": "Colorado Office of Behavioral Health",
        "abbreviation": "OBH",
        "division": "Department of Human Services",
        "website": "https://bha.colorado.gov/",
        "data_portal": "https://bha.colorado.gov/provider-directory",
        "data_access": "searchable_database",
        "licenses": ["SUD", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(303) 866-5700"
        }
    },
    
    "CT": {
        "name": "Connecticut Department of Mental Health and Addiction Services",
        "abbreviation": "DMHAS",
        "website": "https://portal.ct.gov/DMHAS",
        "data_portal": "https://portal.ct.gov/DMHAS/Programs-and-Services/Finding-Services/Finding-Services",
        "data_access": "provider_search",
        "licenses": ["Mental Health", "Substance Abuse", "Co-occurring"],
        "contact": {
            "phone": "(860) 418-7000"
        }
    },
    
    "DE": {
        "name": "Delaware Division of Substance Abuse and Mental Health",
        "abbreviation": "DSAMH",
        "division": "Department of Health and Social Services",
        "website": "https://dhss.delaware.gov/dsamh/",
        "data_portal": "https://dhss.delaware.gov/dsamh/services.html",
        "data_access": "provider_directory",
        "licenses": ["Substance Abuse", "Mental Health"],
        "contact": {
            "phone": "(302) 255-9399"
        }
    },
    
    "FL": {
        "name": "Florida Department of Children and Families",
        "abbreviation": "DCF",
        "division": "Substance Abuse and Mental Health",
        "website": "https://www.myflfamilies.com/",
        "data_portal": "https://www.myflfamilies.com/service-programs/samh/",
        "data_access": "provider_search",
        "licenses": ["Substance Abuse", "Mental Health", "Detox"],
        "managing_entities": "https://www.myflfamilies.com/service-programs/samh/managing-entities.shtml",
        "contact": {
            "phone": "(850) 487-1111"
        }
    },
    
    "GA": {
        "name": "Georgia Department of Behavioral Health and Developmental Disabilities",
        "abbreviation": "DBHDD",
        "website": "https://dbhdd.georgia.gov/",
        "data_portal": "https://dbhdd.georgia.gov/bh-providers",
        "data_access": "provider_directory",
        "licenses": ["Mental Health", "Addictive Diseases", "Developmental Disabilities"],
        "contact": {
            "phone": "(404) 657-2252"
        }
    },
    
    "HI": {
        "name": "Hawaii Department of Health",
        "abbreviation": "DOH",
        "division": "Alcohol and Drug Abuse Division",
        "website": "https://health.hawaii.gov/",
        "data_portal": "https://health.hawaii.gov/substance-abuse/",
        "data_access": "provider_list",
        "licenses": ["Substance Abuse Treatment", "Mental Health"],
        "contact": {
            "phone": "(808) 692-7506"
        }
    },
    
    "ID": {
        "name": "Idaho Department of Health and Welfare",
        "abbreviation": "IDHW",
        "division": "Division of Behavioral Health",
        "website": "https://healthandwelfare.idaho.gov/",
        "data_portal": "https://healthandwelfare.idaho.gov/services-programs/behavioral-health",
        "data_access": "provider_search",
        "licenses": ["SUD Treatment", "Mental Health"],
        "contact": {
            "phone": "(208) 334-5500"
        }
    },
    
    "IL": {
        "name": "Illinois Division of Substance Use Prevention and Recovery",
        "abbreviation": "SUPR",
        "division": "Department of Human Services",
        "website": "https://www.dhs.state.il.us/page.aspx?item=29725",
        "data_portal": "https://www.dhs.state.il.us/page.aspx?item=86064",
        "data_access": "provider_locator",
        "licenses": ["Substance Use Disorder", "DUI", "Recovery Homes"],
        "contact": {
            "phone": "(312) 814-3840"
        }
    },
    
    "IN": {
        "name": "Indiana Division of Mental Health and Addiction",
        "abbreviation": "DMHA",
        "division": "Family and Social Services Administration",
        "website": "https://www.in.gov/fssa/dmha/",
        "data_portal": "https://www.in.gov/fssa/dmha/find-help/",
        "data_access": "provider_search",
        "licenses": ["Addiction Treatment", "Mental Health", "Recovery Residences"],
        "contact": {
            "phone": "(317) 232-7800"
        }
    },
    
    "IA": {
        "name": "Iowa Department of Health and Human Services",
        "abbreviation": "HHS",
        "division": "Bureau of Substance Abuse",
        "website": "https://hhs.iowa.gov/",
        "data_portal": "https://hhs.iowa.gov/substance-use/providers",
        "data_access": "provider_list",
        "licenses": ["Substance Abuse Treatment", "Mental Health"],
        "contact": {
            "phone": "(515) 281-5021"
        }
    },
    
    "KS": {
        "name": "Kansas Department for Aging and Disability Services",
        "abbreviation": "KDADS",
        "division": "Behavioral Health Services",
        "website": "https://kdads.ks.gov/",
        "data_portal": "https://kdads.ks.gov/commissions/behavioral-health",
        "data_access": "provider_directory",
        "licenses": ["SUD", "Mental Health", "Problem Gambling"],
        "contact": {
            "phone": "(785) 296-3773"
        }
    },
    
    "KY": {
        "name": "Kentucky Department for Behavioral Health, Developmental and Intellectual Disabilities",
        "abbreviation": "DBHDID",
        "division": "Cabinet for Health and Family Services",
        "website": "https://dbhdid.ky.gov/",
        "data_portal": "https://dbhdid.ky.gov/kdbhdid/default.aspx",
        "data_access": "provider_search",
        "licenses": ["Substance Abuse", "Mental Health", "Developmental Disabilities"],
        "contact": {
            "phone": "(502) 564-4456"
        }
    },
    
    "LA": {
        "name": "Louisiana Department of Health",
        "abbreviation": "LDH",
        "division": "Office of Behavioral Health",
        "website": "https://ldh.la.gov/",
        "data_portal": "https://ldh.la.gov/index.cfm/directory/category/186",
        "data_access": "provider_directory",
        "licenses": ["Behavioral Health", "Addictive Disorders"],
        "contact": {
            "phone": "(225) 342-9500"
        }
    },
    
    "ME": {
        "name": "Maine Department of Health and Human Services",
        "abbreviation": "DHHS",
        "division": "Office of Behavioral Health",
        "website": "https://www.maine.gov/dhhs/",
        "data_portal": "https://www.maine.gov/dhhs/obh/providers",
        "data_access": "provider_list",
        "licenses": ["Substance Use Disorder", "Mental Health"],
        "contact": {
            "phone": "(207) 287-2595"
        }
    },
    
    "MD": {
        "name": "Maryland Behavioral Health Administration",
        "abbreviation": "BHA",
        "division": "Department of Health",
        "website": "https://health.maryland.gov/bha/",
        "data_portal": "https://health.maryland.gov/bha/Pages/Provider-Search.aspx",
        "data_access": "provider_search",
        "licenses": ["SUD", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(410) 402-8300"
        }
    },
    
    "MA": {
        "name": "Massachusetts Department of Public Health",
        "abbreviation": "DPH",
        "division": "Bureau of Substance Addiction Services",
        "website": "https://www.mass.gov/orgs/bureau-of-substance-addiction-services",
        "data_portal": "https://www.mass.gov/info-details/find-substance-use-treatment",
        "data_access": "searchable_database",
        "licenses": ["Substance Use Disorder", "Recovery Support"],
        "special_programs": {
            "MASH": "https://www.masshousingregistry.org/",
            "TSS": "Treatment Services System"
        },
        "contact": {
            "phone": "(617) 624-5111"
        }
    },
    
    "MI": {
        "name": "Michigan Department of Licensing and Regulatory Affairs",
        "abbreviation": "LARA",
        "division": "Bureau of Professional Licensing",
        "website": "https://www.michigan.gov/lara",
        "data_portal": "https://www.michigan.gov/lara/bureau-list/bpl",
        "data_access": "license_verification",
        "licenses": ["Substance Use Disorder", "Residential", "Outpatient"],
        "behavioral_health": "https://www.michigan.gov/mdhhs/keep-mi-healthy/mentalhealth",
        "contact": {
            "phone": "(517) 335-1980"
        }
    },
    
    "MN": {
        "name": "Minnesota Department of Human Services",
        "abbreviation": "DHS",
        "division": "Behavioral Health Division",
        "website": "https://mn.gov/dhs/",
        "data_portal": "https://mn.gov/dhs/people-we-serve/adults/health-care/substance-abuse/programs-services/",
        "data_access": "provider_search",
        "licenses": ["Chemical Dependency", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(651) 431-2000"
        }
    },
    
    "MS": {
        "name": "Mississippi Department of Mental Health",
        "abbreviation": "DMH",
        "division": "Bureau of Alcohol and Drug Services",
        "website": "https://www.dmh.ms.gov/",
        "data_portal": "https://www.dmh.ms.gov/service-options/",
        "data_access": "provider_directory",
        "licenses": ["Substance Abuse", "Mental Health"],
        "contact": {
            "phone": "(601) 359-1288"
        }
    },
    
    "MO": {
        "name": "Missouri Department of Mental Health",
        "abbreviation": "DMH",
        "division": "Division of Behavioral Health",
        "website": "https://dmh.mo.gov/",
        "data_portal": "https://dmh.mo.gov/behavioral-health/treatment-services",
        "data_access": "provider_search",
        "licenses": ["Substance Use Disorder", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(573) 751-4942"
        }
    },
    
    "MT": {
        "name": "Montana Department of Public Health and Human Services",
        "abbreviation": "DPHHS",
        "division": "Behavioral Health and Developmental Disabilities Division",
        "website": "https://dphhs.mt.gov/",
        "data_portal": "https://dphhs.mt.gov/amdd/substanceuse/treatment",
        "data_access": "provider_list",
        "licenses": ["Chemical Dependency", "Mental Health"],
        "contact": {
            "phone": "(406) 444-5622"
        }
    },
    
    "NE": {
        "name": "Nebraska Division of Behavioral Health",
        "abbreviation": "DBH",
        "division": "Department of Health and Human Services",
        "website": "https://dhhs.ne.gov/Pages/Behavioral-Health.aspx",
        "data_portal": "https://dhhs.ne.gov/Pages/Behavioral-Health-Provider-Search.aspx",
        "data_access": "provider_search",
        "licenses": ["Substance Abuse", "Mental Health"],
        "contact": {
            "phone": "(402) 471-8553"
        }
    },
    
    "NV": {
        "name": "Nevada Division of Public and Behavioral Health",
        "abbreviation": "DPBH",
        "division": "Department of Health and Human Services",
        "website": "https://dpbh.nv.gov/",
        "data_portal": "https://dpbh.nv.gov/Programs/ClinicalBH/ClinicalBH_Home/",
        "data_access": "certification_list",
        "licenses": ["Substance Abuse Treatment", "Mental Health", "Co-occurring"],
        "sapta": "https://dpbh.nv.gov/Programs/ClinicalSAPTA/Clinical_SAPTA-Home/",
        "contact": {
            "phone": "(775) 684-4200"
        }
    },
    
    "NH": {
        "name": "New Hampshire Bureau of Drug and Alcohol Services",
        "abbreviation": "BDAS",
        "division": "Department of Health and Human Services",
        "website": "https://www.dhhs.nh.gov/",
        "data_portal": "https://www.dhhs.nh.gov/programs-services/behavioral-health",
        "data_access": "provider_list",
        "licenses": ["Substance Use Disorder", "Mental Health"],
        "contact": {
            "phone": "(603) 271-6738"
        }
    },
    
    "NJ": {
        "name": "New Jersey Division of Mental Health and Addiction Services",
        "abbreviation": "DMHAS",
        "division": "Department of Human Services",
        "website": "https://www.nj.gov/humanservices/dmhas/home/",
        "data_portal": "https://njsams.rutgers.edu/TreatmentDirectory/",
        "data_access": "searchable_database",
        "licenses": ["Substance Abuse", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(609) 292-3717"
        }
    },
    
    "NM": {
        "name": "New Mexico Behavioral Health Services Division",
        "abbreviation": "BHSD",
        "division": "Human Services Department",
        "website": "https://www.hsd.state.nm.us/behavioral-health-services-division/",
        "data_portal": "https://www.hsd.state.nm.us/lookingforassistance/",
        "data_access": "provider_directory",
        "licenses": ["Behavioral Health", "Substance Abuse"],
        "contact": {
            "phone": "(505) 476-9266"
        }
    },
    
    "NY": {
        "name": "New York Office of Addiction Services and Supports",
        "abbreviation": "OASAS",
        "website": "https://oasas.ny.gov/",
        "data_portal": "https://webapps.oasas.ny.gov/providerDirectory/index.cfm",
        "data_access": "searchable_database_export",
        "licenses": ["Chemical Dependence", "Outpatient", "Residential", "Opioid Treatment"],
        "omh": "https://www.omh.ny.gov/",  # Office of Mental Health separate
        "contact": {
            "phone": "(518) 473-3460"
        }
    },
    
    "NC": {
        "name": "North Carolina Division of Mental Health, Developmental Disabilities and Substance Abuse Services",
        "abbreviation": "DMH/DD/SAS",
        "division": "Department of Health and Human Services",
        "website": "https://www.ncdhhs.gov/divisions/mhddsas",
        "data_portal": "https://www.ncdhhs.gov/providers/provider-info/mental-health-developmental-disabilities-and-substance-abuse-services",
        "data_access": "provider_directory",
        "licenses": ["Mental Health", "Substance Abuse", "Developmental Disabilities"],
        "contact": {
            "phone": "(919) 733-7011"
        }
    },
    
    "ND": {
        "name": "North Dakota Behavioral Health Division",
        "abbreviation": "BHD",
        "division": "Department of Health and Human Services",
        "website": "https://www.hhs.nd.gov/",
        "data_portal": "https://www.behavioralhealth.nd.gov/addiction/treatment",
        "data_access": "provider_list",
        "licenses": ["Substance Use Disorder", "Mental Health"],
        "contact": {
            "phone": "(701) 328-8920"
        }
    },
    
    "OH": {
        "name": "Ohio Department of Mental Health and Addiction Services",
        "abbreviation": "OhioMHAS",
        "website": "https://mha.ohio.gov/",
        "data_portal": "https://mha.ohio.gov/find-help",
        "data_access": "provider_locator",
        "licenses": ["Mental Health", "Addiction Services", "Residential", "Outpatient"],
        "certification": "https://mha.ohio.gov/community-partners/certifications",
        "contact": {
            "phone": "(614) 466-2596"
        }
    },
    
    "OK": {
        "name": "Oklahoma Department of Mental Health and Substance Abuse Services",
        "abbreviation": "ODMHSAS",
        "website": "https://oklahoma.gov/odmhsas.html",
        "data_portal": "https://oklahoma.gov/odmhsas/treatment/find-services.html",
        "data_access": "provider_search",
        "licenses": ["Substance Abuse", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(405) 522-3908"
        }
    },
    
    "OR": {
        "name": "Oregon Health Authority",
        "abbreviation": "OHA",
        "division": "Behavioral Health Division",
        "website": "https://www.oregon.gov/oha/",
        "data_portal": "https://www.oregon.gov/oha/HSD/AMH/Pages/Provider-Search.aspx",
        "data_access": "provider_search",
        "licenses": ["Substance Use Disorder", "Mental Health", "Problem Gambling"],
        "contact": {
            "phone": "(503) 945-5944"
        }
    },
    
    "PA": {
        "name": "Pennsylvania Department of Drug and Alcohol Programs",
        "abbreviation": "DDAP",
        "website": "https://www.ddap.pa.gov/",
        "data_portal": "https://sais.health.pa.gov/commonpoc/content/publiccommonpoc/normalSearch.aspx",
        "data_access": "searchable_portal",
        "licenses": ["Drug and Alcohol", "Narcotic Treatment", "Residential", "Outpatient"],
        "get_help": "https://apps.ddap.pa.gov/GetHelpNow/",
        "contact": {
            "phone": "(717) 736-7459"
        }
    },
    
    "RI": {
        "name": "Rhode Island Department of Behavioral Healthcare, Developmental Disabilities and Hospitals",
        "abbreviation": "BHDDH",
        "website": "https://bhddh.ri.gov/",
        "data_portal": "https://bhddh.ri.gov/substance-use/find-treatment",
        "data_access": "provider_directory",
        "licenses": ["Substance Use", "Mental Health", "Developmental Disabilities"],
        "contact": {
            "phone": "(401) 462-3201"
        }
    },
    
    "SC": {
        "name": "South Carolina Department of Alcohol and Other Drug Abuse Services",
        "abbreviation": "DAODAS",
        "website": "https://daodas.sc.gov/",
        "data_portal": "https://daodas.sc.gov/treatment/",
        "data_access": "county_directory",
        "licenses": ["Substance Abuse Prevention and Treatment"],
        "dmh": "https://scdmh.net/",  # Department of Mental Health separate
        "contact": {
            "phone": "(803) 896-5555"
        }
    },
    
    "SD": {
        "name": "South Dakota Division of Behavioral Health",
        "abbreviation": "DBH",
        "division": "Department of Social Services",
        "website": "https://dss.sd.gov/behavioralhealth/",
        "data_portal": "https://dss.sd.gov/behavioralhealth/community/treatmentprevention.aspx",
        "data_access": "provider_list",
        "licenses": ["Substance Abuse", "Mental Health"],
        "contact": {
            "phone": "(605) 367-5236"
        }
    },
    
    "TN": {
        "name": "Tennessee Department of Mental Health and Substance Abuse Services",
        "abbreviation": "TDMHSAS",
        "website": "https://www.tn.gov/behavioral-health.html",
        "data_portal": "https://www.tn.gov/behavioral-health/substance-abuse-services/treatment.html",
        "data_access": "provider_directory",
        "licenses": ["Substance Abuse", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(615) 532-6500"
        }
    },
    
    "TX": {
        "name": "Texas Health and Human Services Commission",
        "abbreviation": "HHSC",
        "division": "Behavioral Health Services",
        "website": "https://www.hhs.texas.gov/",
        "data_portal": "https://www.hhs.texas.gov/providers/long-term-care-providers/search-texas-providers",
        "data_access": "searchable_database_export",
        "licenses": ["Chemical Dependency", "Mental Health", "Co-occurring"],
        "substance_use": "https://www.hhs.texas.gov/services/mental-health-substance-use",
        "contact": {
            "phone": "(877) 787-8999"
        }
    },
    
    "UT": {
        "name": "Utah Division of Substance Abuse and Mental Health",
        "abbreviation": "DSAMH",
        "division": "Department of Health and Human Services",
        "website": "https://dsamh.utah.gov/",
        "data_portal": "https://dsamh.utah.gov/providers/",
        "data_access": "provider_list",
        "licenses": ["Substance Use Disorder", "Mental Health"],
        "contact": {
            "phone": "(801) 538-3939"
        }
    },
    
    "VT": {
        "name": "Vermont Department of Health",
        "abbreviation": "VDH",
        "division": "Division of Alcohol and Drug Abuse Programs",
        "website": "https://www.healthvermont.gov/",
        "data_portal": "https://www.healthvermont.gov/alcohol-drugs/services/treatment-recovery",
        "data_access": "provider_directory",
        "licenses": ["Substance Abuse Treatment", "Recovery Centers"],
        "contact": {
            "phone": "(802) 863-7200"
        }
    },
    
    "VA": {
        "name": "Virginia Department of Behavioral Health and Developmental Services",
        "abbreviation": "DBHDS",
        "website": "https://dbhds.virginia.gov/",
        "data_portal": "https://dbhds.virginia.gov/office-of-licensing/search-for-providers/",
        "data_access": "provider_search",
        "licenses": ["Substance Abuse", "Mental Health", "Developmental Services"],
        "contact": {
            "phone": "(804) 786-3921"
        }
    },
    
    "WA": {
        "name": "Washington State Department of Health",
        "abbreviation": "DOH",
        "division": "Behavioral Health Administration",
        "website": "https://www.doh.wa.gov/",
        "data_portal": "https://www.doh.wa.gov/LicensesPermitsandCertificates/FacilitiesNewReneworUpdate/BehavioralHealthAgencies",
        "data_access": "license_lookup",
        "licenses": ["Behavioral Health Agency", "Substance Use Disorder", "Mental Health"],
        "hca": "https://www.hca.wa.gov/",  # Health Care Authority also involved
        "contact": {
            "phone": "(360) 236-4700"
        }
    },
    
    "WV": {
        "name": "West Virginia Bureau for Behavioral Health",
        "abbreviation": "BBH",
        "division": "Department of Health and Human Resources",
        "website": "https://dhhr.wv.gov/bbh/",
        "data_portal": "https://dhhr.wv.gov/bbh/Pages/Provider-Search.aspx",
        "data_access": "provider_search",
        "licenses": ["Substance Use Disorder", "Mental Health"],
        "contact": {
            "phone": "(304) 356-4811"
        }
    },
    
    "WI": {
        "name": "Wisconsin Department of Health Services",
        "abbreviation": "DHS",
        "division": "Division of Care and Treatment Services",
        "website": "https://www.dhs.wisconsin.gov/",
        "data_portal": "https://www.dhs.wisconsin.gov/aoda/treatment-locator.htm",
        "data_access": "treatment_locator",
        "licenses": ["AODA", "Mental Health", "Co-occurring"],
        "certification": "https://www.dhs.wisconsin.gov/aoda/certification.htm",
        "contact": {
            "phone": "(608) 266-2717"
        }
    },
    
    "WY": {
        "name": "Wyoming Department of Health",
        "abbreviation": "WDH",
        "division": "Behavioral Health Division",
        "website": "https://health.wyo.gov/",
        "data_portal": "https://health.wyo.gov/behavioralhealth/treatment/",
        "data_access": "provider_list",
        "licenses": ["Substance Abuse", "Mental Health"],
        "contact": {
            "phone": "(307) 777-6494"
        }
    },
    
    "DC": {
        "name": "DC Department of Behavioral Health",
        "abbreviation": "DBH",
        "website": "https://dbh.dc.gov/",
        "data_portal": "https://dbh.dc.gov/service/substance-abuse-treatment",
        "data_access": "provider_directory",
        "licenses": ["Substance Use Disorder", "Mental Health", "Co-occurring"],
        "contact": {
            "phone": "(202) 673-2200"
        }
    }
}

def get_state_agency(state_code: str) -> dict:
    """Get agency information for a specific state"""
    return STATE_AGENCIES.get(state_code.upper(), {})

def get_states_by_data_access(access_type: str) -> list:
    """Get states that have a specific type of data access"""
    states = []
    for state_code, info in STATE_AGENCIES.items():
        if info.get("data_access") == access_type:
            states.append(state_code)
    return sorted(states)

def get_states_with_api() -> list:
    """Get states that have API access"""
    states = []
    for state_code, info in STATE_AGENCIES.items():
        if info.get("api_available") or "api" in info.get("data_access", ""):
            states.append(state_code)
    return sorted(states)

def get_states_with_download() -> list:
    """Get states that allow data download (Excel, CSV, etc.)"""
    download_types = ["excel_download", "csv_download", "searchable_database_export"]
    states = []
    for state_code, info in STATE_AGENCIES.items():
        if info.get("data_access") in download_types:
            states.append(state_code)
    return sorted(states)

def generate_state_summary():
    """Generate summary statistics about state data access"""
    access_types = {}
    total_states = len(STATE_AGENCIES)
    
    for state_code, info in STATE_AGENCIES.items():
        access_type = info.get("data_access", "unknown")
        access_types[access_type] = access_types.get(access_type, 0) + 1
    
    print(f"=== State Agency Data Access Summary ===")
    print(f"Total states/territories: {total_states}")
    print(f"\nData Access Methods:")
    for access_type, count in sorted(access_types.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_states) * 100
        print(f"  {access_type}: {count} states ({percentage:.1f}%)")
    
    print(f"\nStates with APIs: {len(get_states_with_api())}")
    print(f"States with downloads: {len(get_states_with_download())}")
    
    print(f"\nPriority states (with easy data access):")
    for state in get_states_with_download() + get_states_with_api():
        if state not in get_states_with_download():
            continue
        agency = STATE_AGENCIES[state]
        print(f"  {state} - {agency['name']} ({agency['data_access']})")

if __name__ == "__main__":
    generate_state_summary()