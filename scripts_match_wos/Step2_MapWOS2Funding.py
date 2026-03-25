import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
import numpy as np
import csv
import html

import pyarrow as pa
import pyarrow.parquet as pq
import pycountry
from collections import Counter

# General
openAlexPath = Path("/mnt/openalex/OpenAlex/openalex-snapshot")  # Path to OpenAlex data

# INPUTS
dataPath = Path('Data')

oaInstitutionsPath = dataPath/"openalex_institutions"/"institutions_core+basic+alternate_names+ids+geo+urls+roles+metrics.tsv"
oaFundersPath = dataPath/"openalex_institutions"/"funders_core+basic+ids+extra+counts.tsv"

wosFundingPath = "Data/WOS_TSV_FUNDING_2023.feather"
mapWithOrgsPath = "Data/wos2jins_2023_duplicates-20251023.feather"

wosAddressesPath = "Data/wos_authors_adresses.parquet"

# OUTPUTS
outputPath = Path("./") # ./ = current directory
papersMatchPath = outputPath / "Results/grant_institutions_jin_2023_affiliation_match_20251023.feather"
agenciesMatchPath = outputPath / "Results/grant_agency_matched_jin_2023_affiliation_match_20251023.feather"
papersMatchPathParquet = papersMatchPath.with_suffix('.parquet')
agenciesMatchPathParquet = agenciesMatchPath.with_suffix('.parquet')




useAffiliationCountry = True

oaInstitutions = pd.read_csv(oaInstitutionsPath, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE, doublequote=False, escapechar='\\',dtype={"id":str})
oaFunders = pd.read_csv(oaFundersPath, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE, doublequote=False, escapechar='\\',dtype={"id":str})

import openalexraw

oa = openalexraw.OpenAlex(
    openAlexPath = openAlexPath
    #Path to OpenAlex data
)




entityType = "institutions"
entitiesCount = oa.getRawEntityCount(entityType)


gid2Count = {}
for id, works_count in oaInstitutions[['id','summary_stats:works_count']].itertuples(index=False):
    gid2Count["OA_"+id] = works_count


for id, works_count in oaFunders[['id','works_count']].itertuples(index=False):
    gid2Count["OA_"+id] = works_count



manualCountryMapping = {
    "Russia": "RU",
    "United States of America": "US",
    "United Kingdom": "GB",
    "Turkey": "TR",
    "Aland Islands": "AX",
    "Bonaire Sint Eustatius and Saba": "BQ",
    "Brunei": "BN",
    "Congo Republic": "CG",
    "Democratic Republic of the Congo": "CD",
    "DR Congo": "CD",
    "Curacao": "CW",
    "Falkland Islands": "FK",
    "Ivory Coast": "CI",
    "Kosovo": "XK",
    "Micronesia": "FM",
    "Palestine": "PS",
    "Palestinian Territory": "PS",
    "Reunion": "RE",
    "Sint Maarten": "SX",
    "St Kitts and Nevis": "KN",
    "St Vincent and Grenadines": "VC",
    "The Gambia": "GM",
    "The Netherlands": "NL",
    "Turkey": "TR",
    "US Virgin Islands": "VI",
    "Vatican": "VA",
    "Vatican City": "VA",
    "Bonaire": "BQ",
    "Bosnia & Herceg": "BA",
    "Cape Verde": "CV",
    "Cent Afr Republ": "CF",
    "Cote Ivoire": "CI",
    "DEM REP CONGO": "CD",
    "Dominican Rep": "DO",
    "England": "GB",
    "Equat Guinea": "GQ",
    "Falkland Island": "FK",
    "Guinea Bissau": "GW",
    "Macedonia": "MK",
    "Marshall Island": "MH",
    "Neth Antilles": "AN",
    "North Ireland": "GB",
    "Papua N Guinea": "PG",
    "Peoples R China": "CN",
    "Rep Congo": "CG",
    "Sao Tome & Prin": "ST",
    "Scotland": "GB",
    "St Kitts & Nevi": "KN",
    "St Lucia": "LC",
    "St Vincent": "VC",
    "Swaziland": "SZ",
    "Trinidad Tobago": "TT",
    "U Arab Emirates": "AE",
    "Wales": "GB",
    "Yugoslavia": "YU",
    "*South Korea": "KR",
    "?England": "GB",
    "?India": "IN",
    "?Liechtenstein": "LI",
    "Africa": "AF",
    "Antigua & Barbu": "AG",
    "Ascension Isl": "AC",
    "Australia": "AU",
    "Bangladesh": "BD",
    "Belgium": "BE",
    "Brazil": "BR",
    "Brazil?": "BR",
    "Brit Ind Ocean": "IO",
    "British Virgin Isl": "VG",
    "Bulgaria": "BG",
    "Costa Rica": "CR",
    "Cote d'Ivoire": "CI",
    "Denmark": "DK",
    "England": "GB",
    "France": "FR",
    "Germany": "DE",
    "Germany;": "DE",
    "Greece;": "GR",
    "Hungary;": "HU",
    "India;": "IN",
    "Indonesia;": "ID",
    "Iran,": "IR",
    "Iran, Islamic R": "IR",
    "Iran.": "IR",
    "Iran=": "IR",
    "Italy;": "IT",
    "Jan Mayen": "SJ",
    "Japan;": "JP",
    "Korea, Republic": "KR",
    "Lebanon;": "LB",
    "Malaysia;": "MY",
    "Moldova;": "MD",
    "Netherlands;": "NL",
    "Pakistan;": "PK",
    "Portugal*": "PT",
    "Portugal;": "PT",
    "Russia;": "RU",
    "Saudi Arabia.": "SA",
    "Scotland?": "GB",
    "Serbia Monteneg": "ME",
    "South Korea,": "KR",
    "South Korea.": "KR",
    "Spain;": "ES",
    "St Barthelemy": "BL",
    "St Helena": "SH",
    "St Martin": "MF",
    "Svalbard": "SJ",
    "Sweden;": "SE",
    "Switzerland;": "CH",
    "Tahiti": "PF",
    "Taiwan,": "TW",
    "Taiwan, Provinc": "TW",
    "Taiwan;": "TW",
    "Taiwan\\": "TW",
    "Tanzania,": "TZ",
    "Thailand;": "TH",
    "Tokelau Islands": "TK",
    "Tristan da Cunh": "TC",
    "Tukiye": "TR",
    "Tunisia;": "TN",
    "Turkey*": "TR",
    "Turkey;": "TR",
    "Turkiye": "TR",
    "Turks & Caicos": "TC",
    "Uganda;": "UG",
    "Ukraine)": "UA",
    "Vietnam;": "VN",
    "|Ira": "IR",
}

# european union country codes:
EU_FUNDING_COUNTRY_CODES = {
    # EU members
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE",
    # Associated / eligible
    "IS", "LI", "NO",  # EEA
    "AL", "BA", "MK", "ME", "RS",  # Western Balkans
    "AM", "GE", "MD", "UA",        # Eastern Partnership
    "IL", "TN", "TR",              # Mediterranean / Middle East
    "FO",                          # Faroe Islands
    "GB",                          # United Kingdom (historical)
    "CH",                          # Switzerland (partial)
}

import re
def standardizeCountryNames(country_name):
    country_name = country_name.strip()
    # only keep alphanumeric characters and spaces
    # But I want to keep accented characters, hiphen
    country_name = re.sub(r"[^a-zA-Z0-9À-ž \-&]", "", country_name)
    if country_name in manualCountryMapping:
        return manualCountryMapping[country_name]
    try:
        return pycountry.countries.lookup(country_name).alpha_2
    except LookupError:
        return None

import ast
oaID2Countries = {}
allPossibleCountriesInstitutions = set()
allPossibleCountriesFunders = set()
problematicCountries = set()
for id, country_name, in zip(
        oaInstitutions['id'].to_list(),
        oaInstitutions['geo:country'].to_list(),
    ):
    if(pd.isna(country_name) or country_name == ""):
        continue
    oaID2Countries[id] = standardizeCountryNames(country_name)
    if(oaID2Countries[id] is None):
        problematicCountries.add(country_name)
    allPossibleCountriesInstitutions.add(country_name)


for id, country_code in zip(
        oaFunders['id'].to_list(),
        oaFunders['country_code'].to_list(),
    ):
    if(pd.isna(country_code) or country_code == ""):
        continue
    oaID2Countries[id] = country_code
    allPossibleCountriesFunders.add(country_code)



# Create a mapping from OpenAlex IDs to names
# turn into strings
oaID2Countries = {"OA_"+k:str(v) for k, v in oaID2Countries.items()}

# get all institutions from BE with EU or Europe in their names
oaInstitutionsData = zip (
        oaInstitutions['id'].to_list(),
        oaInstitutions['display_name'].to_list(),
        oaInstitutions['display_name_alternatives'].to_list(),
        oaInstitutions['geo:country_code'].to_list(),
    )
oaFundersData = zip (
        oaFunders['id'].to_list(),
        oaFunders['display_name'].to_list(),
        oaFunders['alternate_titles'].to_list(),
        oaFunders['country_code'].to_list(),
    )
    
for oaData in [oaInstitutionsData, oaFundersData]:
    for id,name,alternative_names,country_code in oaData:
        if(pd.isna(country_code) or country_code != "BE"):
            continue
        nameLower = name.lower()
        if("europ" in nameLower):
            # idsForEU.add("OA_"+id)
            print(f"\"OA_{id}\", # 1 based on name {name}")
            continue
        if(pd.isna(alternative_names)):
            continue
        try:
            altNamesList = ast.literal_eval(alternative_names)
        except:
            continue
        for altName in altNamesList:
            altNameLower = altName.lower()
            if("europ" in altNameLower):
                # idsForEU.add("OA_"+id)
                print(f"\"OA_{id}\", # 2 based on alt name {altName}")
                break
            elif("eu" == altNameLower):
                # idsForEU.add("OA_"+id)
                print(f"\"OA_{id}\", # 3 based on alt name {altName}")
                break


EUInstitutions = {
    "OA_4210088126", # 1 based on name European Federation of Agencies and Regions for Energy and Environment
    "OA_4210097268", # 1 based on name European Association for the Defence of Human Rights
    "OA_4210107986", # 1 based on name European State Forest Association
    "OA_4210136590", # 1 based on name European Aquaculture Technology and Innovation Platform
    "OA_4210145796", # 1 based on name European Heat Pump Association
    "OA_4210106847", # 1 based on name European Network of Building Research Institutes
    "OA_4210122997", # 1 based on name Federation of the European Sporting Goods Industry
    "OA_4210124547", # 1 based on name European Biomass Association
    "OA_4210153951", # 1 based on name Federation of European Aquaculture Producers
    "OA_4210144101", # 1 based on name European Committee of Environmental Technology Suppliers Associations
    "OA_4210103158", # 1 based on name European Business Register
    "OA_4210109849", # 1 based on name European Small Hydropower Association
    "OA_4210120654", # 1 based on name European Union Road Federation
    "OA_4210157815", # 1 based on name European Utilities Telecom Council
    "OA_4210163219", # 1 based on name European Smart Card Industry Association
    "OA_4210165373", # 1 based on name European Virtual Institute on Knowledge-based Multifunctional Materials
    "OA_4210163948", # 2 based on alt name Council of European Employers of the Metal, Engineering and Technology-based industries
    "OA_4210091894", # 1 based on name European Council for Construction Research, Development and Innovation
    "OA_4210132234", # 1 based on name European Coil Coating Association
    "OA_4210117588", # 1 based on name Council of European Aerospace Societies
    "OA_4210146272", # 1 based on name Association of European Science and Technology Transfer Professionals
    "OA_4210160036", # 1 based on name European Plasticisers
    "OA_4210158393", # 1 based on name European Society for Diseases of the Esophagus
    "OA_4210089669", # 1 based on name Confederation of National Associations of Tanners and Dressers of the European Community
    "OA_4210149525", # 1 based on name European Inter-University Association of Society, Science and Technology
    "OA_2801868667", # 1 based on name European Association of Craft, Small and Medium-Sized Enterprises
    "OA_4210143590", # 1 based on name European Communication Research and Education Association
    "OA_4210110775", # 1 based on name European Solar Thermal Electricity Association
    "OA_4210131776", # 1 based on name European Magnetic Field Laboratory
    "OA_4210117635", # 1 based on name Europace
    "OA_4210138810", # 1 based on name Business Europe
    "OA_2799634458", # 1 based on name European Association of Development Agencies
    "OA_4210122548", # 1 based on name European Distribution System Operators for Smart Grids
    "OA_4210091679", # 1 based on name European Conference of Transport Research Institutes
    "OA_4210121013", # 1 based on name European Association for Forwarding, Transport, Logistics and Customs Services
    "OA_4210126979", # 1 based on name Confederation of European Forest Owners
    "OA_4210147269", # 1 based on name European Association for the Promotion of Cogeneration
    "OA_2800014257", # 1 based on name European Photovoltaic Industry Association
    "OA_4210104155", # 1 based on name European Association for Battery Hybrid and Fuel Cell Electric Vehicles
    "OA_4210112830", # 1 based on name Churches’ Commission for Migrants in Europe
    "OA_2801357364", # 1 based on name League of European Research Universities
    "OA_2801915573", # 1 based on name European Network on Debt and Development
    "OA_4210106744", # 1 based on name Trans European Policy Studies Association
    "OA_4210129930", # 1 based on name Plastics Recyclers Europe
    "OA_4210153008", # 1 based on name Public Safety Communication Europe Forum
    "OA_4210161992", # 1 based on name European Road Transport Telematics Implementation Co-Ordination
    "OA_4210138375", # 1 based on name European Biodiesel Board
    "OA_4210132335", # 1 based on name Airports Council International Europe
    "OA_4210087862", # 1 based on name European Regional and Local Health Authorities
    "OA_4210126246", # 1 based on name European Community Shipowners' Associations
    "OA_2799408127", # 1 based on name European Trade Union Confederation
    "OA_2802813508", # 1 based on name European Welding Federation
    "OA_4210097805", # 1 based on name IHE Europe
    "OA_4210157282", # 1 based on name European Research Institute of Catalysis
    "OA_4210136994", # 1 based on name Community of European Shipyards Associations
    "OA_2803031397", # 1 based on name European Apparel and Textile Confederation
    "OA_4210148456", # 1 based on name European Passengers' Federation
    "OA_2799484759", # 1 based on name Federation of European Motorcyclists Associations
    "OA_2899149673", # 1 based on name European Peacebuilding Liaison Office
    "OA_4210135535", # 1 based on name Missing Children Europe
    "OA_4210104990", # 1 based on name European Shippers' Council
    "OA_2802630777", # 1 based on name FoodDrinkEurope
    "OA_4210140276", # 1 based on name Institut Interuniversitaire pour les Relations entre l'Europe l'Amérique Latine et les Caraïbes
    "OA_4210120992", # 1 based on name Federation of European Publishers
    "OA_4210151793", # 1 based on name European Genetic Alliances Network
    "OA_4210125389", # 1 based on name European Regions Research and Innovation Network
    "OA_4210139136", # 1 based on name European Conference for AeroSpace Sciences
    "OA_4210104519", # 1 based on name Hydrogen Fuel Cells and Electro-Mobility in European Regions
    "OA_3015934314", # 1 based on name European Council on Refugees and Exiles
    "OA_4210111785", # 1 based on name European Crowdfunding Network
    "OA_4210136382", # 1 based on name European Centre for Knowledge and Technology Transfer
    "OA_4210130345", # 1 based on name European Intermodal Association
    "OA_4210130137", # 1 based on name European Plastics Converters
    "OA_4210123867", # 1 based on name European Energy Research Alliance
    "OA_4210133713", # 1 based on name European Dairy Association
    "OA_4210098829", # 1 based on name European Foundation Centre
    "OA_4210167195", # 1 based on name European Organisation for Security
    "OA_4210151778", # 1 based on name European Membrane House
    "OA_4210136270", # 1 based on name Foundation for European Progressive Studies
    # "OA_4210146964", # 1 based on name Europe Unlimited (Belgium)
    "OA_3200473060", # 1 based on name Network of European Foundations
    "OA_4210098375", # 1 based on name WindEurope
    "OA_4210089365", # 1 based on name Consortium of European Taxonomic Facilities
    "OA_4210164191", # 1 based on name European Biomass Industry Association
    "OA_4210107110", # 1 based on name European Powder Metallugy Association
    "OA_4210122400", # 1 based on name European Coordination Committee of the Radiological, Electromedical and Healthcare IT Industry
    "OA_4210161161", # 1 based on name European Foundation for Alcohol Research
    "OA_2800054806", # 1 based on name European Renewable Energy Council
    # "OA_95094469", # 1 based on name EuropaBio
    "OA_4210142811", # 1 based on name European Transport Safety Council
    "OA_4210144547", # 1 based on name European Network of Science Centres and Museums
    "OA_4210115871", # 1 based on name European Mentoring Coaching Council
    "OA_4210143749", # 1 based on name European Geothermal Energy Council
    # "OA_4210155962", # 1 based on name Greenovate! Europe
    "OA_159526901", # 1 based on name European Committee for Standardization
    "OA_4210156175", # 1 based on name European Scientific Working group on Influenza
    "OA_4210144259", # 1 based on name European Health Telematics Association
    "OA_4210128266", # 1 based on name Technical Support for European Organisations
    "OA_242631042", # 1 based on name European Brain Council
    "OA_4210151635", # 1 based on name Architects Council of Europe
    # "OA_4210160333", # 1 based on name GC Europe (Belgium)
    "OA_4210144095", # 1 based on name European Writers' Council
    "OA_4210126593", # 2 based on alt name European Safety Systems
    "OA_4210129061", # 1 based on name European Parliamentary Forum on Population & Development
    "OA_4210136188", # 1 based on name European Corporate Governance Institute
    "OA_2801222929", # 1 based on name European CanCer Organisation
    "OA_4210109438", # 1 based on name European Society of Intensive Care Medicine
    "OA_4210095793", # 1 based on name European Centre for Ecotoxicology and Toxicology of Chemicals
    "OA_2801332348", # 1 based on name European Platform of Women Scientists
    "OA_4210119240", # 1 based on name Conseil Européen de l'Industrie des Peintures des Encres d'Imprimerie et des Couleurs d'Art
    "OA_2801564582", # 1 based on name European External Action Service
    "OA_4210162738", # 2 based on alt name European Organisation for the Safety of Air Navigation
    "OA_47413983", # 1 based on name European Forum for Good Clinical Practice
    "OA_2802488035", # 1 based on name European Food Information Resource
    "OA_4210152123", # 1 based on name Network of the Universities from the Capitals of Europe
    "OA_4210123756", # 1 based on name European Disability Forum
    "OA_4210108302", # 1 based on name European Association of Research Managers and Administrators
    "OA_4210137388", # 1 based on name Europlasma (Belgium)
    "OA_4210125808", # 1 based on name European Emergency Number Association
    "OA_4210124801", # 1 based on name European Organisation for Research and Treatment of Cancer
    "OA_4210131110", # 1 based on name European Federation of Allergy and Airways Diseases Patients Associations
    "OA_2799966864", # 1 based on name European Institute for Advanced Studies in Management
    "OA_2801350356", # 1 based on name European Research Council
    "OA_4210091886", # 1 based on name The European Academy of Gynaecological Surgery
    "OA_4210123157", # 1 based on name Buildings Performance Institute Europe
    "OA_4210141167", # 1 based on name European Social Observatory
    "OA_4210129853", # 1 based on name European Steel Association
    "OA_4210106795", # 1 based on name European Copper Institute
    "OA_4210154953", # 1 based on name European Federation of Nurses Associations
    "OA_2802949550", # 1 based on name Forum of European National Highway Research Laboratories
    "OA_4210090207", # 1 based on name European Remanufacturing Council
    "OA_4210115330", # 1 based on name European Business and Innovation Centre Network
    "OA_4210125995", # 1 based on name European & International Federation of Natural Stone Industries
    "OA_1320481043", # 1 based on name European Union
    "OA_172363630", # 1 based on name College of Europe
    "OA_2802187282", # 1 based on name European Parliament
    "OA_2801332138", # 1 based on name European Federation of Pharmaceutical Industries and Associations
    "OA_42097590", # 1 based on name Institute for European Environmental Policy
    "OA_4210095307", # 1 based on name Europe Hospitals
    "OA_4210105332", # 1 based on name Association of Schools of Public Health in the European Region
    "OA_4210156341", # 1 based on name AGE Platform Europe
    "OA_4210110296", # 1 based on name European Global Ocean Observing System
    "OA_1293723699", # 1 based on name European Observatory on Health Systems and Policies
    "OA_904910273", # 1 based on name Union des Industries Ferroviaires Européennes
    "OA_4210126181", # 1 based on name Association des Operateurs Postaux Publics Europeens
    "OA_260530381", # 1 based on name European Pathway Association
    "OA_4210164541", # 1 based on name Interface Europe (Belgium)
    "OA_2801048784", # 1 based on name European Health Management Association
    "OA_4210118392", # 1 based on name Sea Europe
    "OA_2799552643", # 1 based on name European Cooperation in Science and Technology
    "OA_4210153574", # 1 based on name European Parkinsons Disease Association
    "OA_2800387288", # 1 based on name European Commission
    "OA_1292404956", # 1 based on name European Union of Medical Specialists
    "OA_4210130123", # 1 based on name European Theoretical Spectroscopy Facility
    "OA_4210086988", # 1 based on name Centre for European Policy Studies
    "OA_4210117623", # 1 based on name European Society of Human Reproduction and Embryology
    "OA_4210117215", # 1 based on name Cosmetics Europe
    "OA_2800837690", # 1 based on name European Food Information Council
    "OA_4210096416", # 1 based on name European Trade Union Institute
    "OA_2799730327", # 1 based on name European Council
    # "OA_4210108252", # 2 based on alt name MSD Europe Belgium NOT EU
    "OA_4210123645", # 1 based on name European Society for Therapeutic Radiology and Oncology
    "OA_929934785", # 1 based on name European Automobile Manufacturers Association
    # "OA_2801148265", # 1 based on name Bio Base Europe NOT EU
    # "OA_4210135891", # 1 based on name PlasticsEurope NOT EU
    "OA_4210155700", # 1 based on name European Underground Research Infrastructure for Disposal of nuclear waste in a Clay Environment
    "OA_4210147753", # 1 based on name Association of European Renewable Energy Research Centres
    "OA_4210129179", # 1 based on name Organisation of European Cancer Institutes
    "OA_4210159695", # 1 based on name Global Alliance of Mental Illness Advocacy Networks-Europe
    # "OA_4210102171", # 1 based on name HOPE European Hospital and Healthcare Federation 
    "OA_338987037", # 1 based on name European University Association
    "OA_2802752048", # 1 based on name European Defence Agency
    "OA_4210122277", # 2 based on alt name European Meteorological Services
    "OA_4210151638", # 1 based on name European Network of Living Labs
    "OA_4210090408", # 2 based on alt name Politique européenne de voisinage et négociations d'élargissement
    "OA_4210087801", # 1 based on name European Society of Biomechanics
    "OA_4210141257", # 1 based on name European Convention for Constructional Steelwork
    "OA_4210142240", # 1 based on name Partnership for Advanced Computing in Europe
    "OA_4210106178", # 1 based on name European Association of Automotive Suppliers
    "OA_4210157179", # 1 based on name European Council for Maritime Applied R&D Association
    "OA_4210101016", # 1 based on name AeroSpace and Defence Industries Association of Europe
    "OA_4210097881", # 1 based on name European Society of Anaesthesiology
    "OA_4210101032", # 1 based on name European Heart Network
    "OA_4210112563", # 1 based on name International Life Sciences Institute Europe
    "OA_4210156108", # 1 based on name European Association for Palliative Care
    "OA_4210104129", # 1 based on name European Society for Paediatric Oncology
    "OA_332397292", # 1 based on name European Resuscitation Council
    "OA_2903168693", # 1 based on name Digital Europe
    "OA_4210120839", # 1 based on name European Cancer Prevention
    "OA_4210092872", # 1 based on name European Chemical Industry Council
    "OA_4210138883", # 1 based on name European Aquaculture Society
    "OA_4210111743", # 1 based on name European Network for Smoking and Tobacco Prevention
    # "OA_4210141690", # 2 based on alt name Agence universitaire de la Francophonie en Europe de l’Ouest
    "OA_2803035357", # 1 based on name Standing Committee of European Doctors
    "OA_4320331514", # 1 based on name European Aeronautics Science Network
}


uid2CountryCode = {}

if(useAffiliationCountry):
    # Load the data
    # Data/wos_authors_adresses.parquet
    wosAuthorsPath = Path(wosAddressesPath)
    parquet_file = pq.ParquetFile(wosAuthorsPath)
    num_row_groups = parquet_file.num_row_groups


    uid2Country = {}
    for i in tqdm(range(num_row_groups), desc="Loading WOS Authors Addresses"):
        df = parquet_file.read_row_group(i).to_pandas()
        # extract uid and country
        df = df[["UID","_seq_no", "country"]]
        # remove rows with nan in country
        df = df.dropna(subset=["country"])
        # remove rows with empty country
        df = df[df["country"] != ""]
        # convert to dict
        for uid, seq_no, country in zip(df["UID"], df["_seq_no"], df["country"]):
            if uid not in uid2Country:
                uid2Country[uid] = []
            uid2Country[uid].append((seq_no, country))
    parquet_file.close()

    problematicCountries = set()
    for key,countryPairs in tqdm(uid2Country.items()):
        # sort by seq_no
        # get largest seq_no
        largest_seq_no = max([seq_no for seq_no, country in countryPairs])
        if(largest_seq_no > 1):
            # replace all with largest seq_no by 1.5 
            countryPairs = [(1.5, country) if seq_no == largest_seq_no else (seq_no, country) for seq_no, country in countryPairs]

        countryPairs.sort(key=lambda x: x[0])

        #convert all countries to their 2-letter codes using standardizeCountryNames
        countryCodes = []
        for seq_no, country in countryPairs:
            country_code = standardizeCountryNames(country)
            if country_code is None:
                problematicCountries.add(country)
            else:
                countryCodes.append(country_code)
        # if there are multiple countries, join them with ;
        uid2CountryCode[key] = countryCodes

    del uid2Country




# Load the data
dfGrantAgency = pd.read_feather(wosFundingPath)

dfGrantMatches = pd.read_feather(mapWithOrgsPath)
# dfGrantMatches = pd.read_feather("Data/wos2jins_2023.feather")

# replace uid with UT
dfGrantAgency = dfGrantAgency.rename(columns={"UID":"UT"})
dfGrantAgency = dfGrantAgency.rename(columns={"agency_name":"grant_agency"})

# preprocess and aggregate them by UT and make string separated by ; if repeated
# oases
# column: is_oa
# wos_grant_agency
# column: grant_agency
# wos_grant_ids
# column: grant_identifier

# remove those with df["grant_agency"] nan
dfGrantAgency = dfGrantAgency.dropna(subset=["grant_agency"])
# remove duplicate uid and grant_agency
dfGrantAgency = dfGrantAgency.drop_duplicates(subset=["UT","grant_agency"])

# remove all columns except UT and the columns listed above
dfGrantAgency = dfGrantAgency[["UT","grant_agency"]]
# expand table, split agencies by ";"
dfGrantAgency = dfGrantAgency.assign(grant_agency=dfGrantAgency['grant_agency'].str.split(';')).explode('grant_agency')

# split by : and get first item
dfGrantAgency['grant_agency'] = dfGrantAgency['grant_agency'].str.split(':').str[0]


dfGrantAgency['grant_agency_normalized'] = dfGrantAgency['grant_agency'].apply(html.unescape)
dfGrantAgency['grant_agency_normalized'] = dfGrantAgency['grant_agency_normalized'].str.replace('&', 'and')
dfGrantAgency['grant_agency_normalized'] = dfGrantAgency['grant_agency_normalized'].str.replace("''", "'")
# trim any leading or trailing whitespace
dfGrantAgency['grant_agency_normalized'] = dfGrantAgency['grant_agency_normalized'].str.strip()
# set it to lowercase
dfGrantAgency['grant_agency_normalized'] = dfGrantAgency['grant_agency_normalized'].str.lower()


dfUniqueGrantsWithCounts = dfGrantAgency.groupby("grant_agency").size().reset_index(name='counts')
dfUniqueGrantsWithCounts = dfUniqueGrantsWithCounts.sort_values(by="counts",ascending=False)


# match dfGrantMatches with dfGrantAgency 
# for dfGrantMatches use grant_agency, for dfGrantAgency use grant_agency_normalized
# rename grant_agency to grant_agency_normalized on dfGrantAgency
dfGrantMatches = dfGrantMatches.rename(columns={"grant_agency":"grant_agency_normalized"})
dfGrantMatches["grant_agency_normalized"] = dfGrantMatches["grant_agency_normalized"].str.strip()
# also lowercase this one
dfGrantMatches["grant_agency_normalized"] = dfGrantMatches["grant_agency_normalized"].str.lower()


# # for each case in which grant_agency_normalized is the same but display_name_y is different
# # this means that multiple display_name_y are mapped to the same grant_agency_normalized
# # so we will use the most common display_name_y based on aggregated "counts" column
# # sum the "counts" for each display_name_y
# display_name_y_counts = dfGrantMatches.groupby("gid")["counts"].sum().reset_index()
# # merge the counts with the original dfGrantMatches
# dfGrantMatchesWithCounts = dfGrantMatches.merge(display_name_y_counts, on="gid", how="left", suffixes=('', '_total'))
# # sort by counts
# dfGrantMatchesWithCounts = dfGrantMatchesWithCounts.sort_values(by="counts_total",ascending=False).reset_index(drop=True)

# if gid is OA_1320481043, replace by OA_4320320300 European Union
dfGrantMatches['gid'] = dfGrantMatches['gid'].replace("OA_1320481043", "OA_4320320300")

# instead use counts from gid2Count if they are available, otherwise set to 0
dfGrantMatches['counts_total'] = dfGrantMatches['gid'].map(lambda x: gid2Count.get(x, 0))
dfGrantMatchesWithCounts = dfGrantMatches.sort_values(by="counts_total",ascending=False).reset_index(drop=True)


# keep only the first display_name_y for each grant_agency_normalized
# dfGrantMatchesWithCounts = dfGrantMatchesWithCounts.loc[dfGrantMatchesWithCounts.groupby("grant_agency_normalized")["counts_total"].idxmax()]


# match to dfGrantAgencyMatched via grant_agency_normalized
# dfGrantMatchesWithCounts = dfGrantMatchesWithCounts.drop_duplicates(subset=["grant_agency_normalized"], keep="first")
# dfGrantAgencyMatched = dfGrantAgency.merge(dfGrantMatchesWithCounts, on="grant_agency_normalized", how="left",
#     suffixes=('', '_matched'))
# instead of just dropping duplicates using the ones with the highest counts,
# we will try to match the country of the funder to the country of the author in the order presented in uid2CountryCode
# first, create a mapping from gid to country code using oaID2Countries
dfGrantMatchesWithCounts['funder_country_code'] = dfGrantMatchesWithCounts['gid'].map(oaID2Countries)
dfGrantMatchesWithCounts["is_eu_funder"] = dfGrantMatchesWithCounts['gid'].map(lambda x: x in EUInstitutions
                                                                               )
agencyNormalizedName2Indices = {}
for index, normalizedName in enumerate(dfGrantMatchesWithCounts['grant_agency_normalized']):
    if normalizedName not in agencyNormalizedName2Indices:
        agencyNormalizedName2Indices[normalizedName] = set()
    agencyNormalizedName2Indices[normalizedName].add(index)


matchedGIDs = []
notFoundStringsCounter = Counter()
gidList = list(dfGrantMatchesWithCounts['gid'])
for index, (normalizedName, wosID) in enumerate(zip(tqdm(dfGrantAgency['grant_agency_normalized']), dfGrantAgency['UT'])):
    if normalizedName not in agencyNormalizedName2Indices:
        notFoundStringsCounter[normalizedName] += 1
        matchedGIDs.append(None)
        continue
    agencyIndices = sorted(list(agencyNormalizedName2Indices[normalizedName]))
    if(len(agencyIndices)==1):
        matchedGIDs.append(gidList[agencyIndices[0]])
        continue
    # if there are multiple indices, we will try to match the country code
    # get the country codes for the agency
    agencyData = dfGrantMatchesWithCounts.loc[agencyIndices]
    # sort by counts_total
    # agencyData = agencyData.sort_values(by="counts_total", ascending=False).reset_index(drop=True)
    # get the first gid

    ##### New CODE
    gidCountryPairs = [(gid,countryCode,isEU) for gid, countryCode, isEU in zip(agencyData['gid'], agencyData['funder_country_code'], agencyData['is_eu_funder'])]
    countryCode2FirstGID = {}
    euGids = []
    for gid, countryCode, isEU in gidCountryPairs:
        # only index valid/non-null country codes
        if countryCode and countryCode not in countryCode2FirstGID:
            countryCode2FirstGID[countryCode] = gid
        if isEU:
            euGids.append(gid)

    # try exact country-code match first
    matchedGID = None
    authorCountryCodes = uid2CountryCode.get(wosID, [])
    for countryCode in authorCountryCodes:
        if countryCode in countryCode2FirstGID:
            matchedGID = countryCode2FirstGID[countryCode]
            break

    # if no exact match and we have EU institution candidates, match any author country in EU_FUNDING_COUNTRY_CODES
    if matchedGID is None and euGids:
        for countryCode in authorCountryCodes:
            if countryCode in EU_FUNDING_COUNTRY_CODES:
                matchedGID = euGids[0]
                break

    # fallback: use the first gid candidate
    if matchedGID is None:
        matchedGID = gidList[agencyIndices[0]]
    matchedGIDs.append(matchedGID)

    ##### OLD CODE for reference
    # gidCountryPairs = [(gid,countryCode,isEU) for gid, countryCode, isEU in zip(agencyData['gid'], agencyData['funder_country_code'], agencyData['is_eu_funder'])]
    # countryCode2FirstGID = {}
    # for gid, countryCode, isEU in gidCountryPairs:
    #     if countryCode not in countryCode2FirstGID:
    #         countryCode2FirstGID[countryCode] = gid
    # # now we have a mapping from country code to first gid
    # # we will try to match the country code of the author to the country code of the agency
    # matchedGID = None
    # for countryCode in uid2CountryCode.get(wosID, []):
    #     if countryCode in countryCode2FirstGID:
    #         matchedGID = countryCode2FirstGID[countryCode]
    #         break
    # if matchedGID is None:
    #     # if no match found, use the first gid
    #     matchedGID = gidList[agencyIndices[0]]
    # matchedGIDs.append(matchedGID)

dfGrantAgency['gid'] = matchedGIDs

gid2Indices = {}
for index, gid in enumerate(dfGrantMatchesWithCounts['gid']):
    if gid not in gid2Indices:
        gid2Indices[gid] = set()
    gid2Indices[gid].add(index)


nonNullFeaturesCount = dfGrantMatchesWithCounts.notnull().sum(axis=1)

gid2BestIndex = {}
# choose the best index based on the amount of non-null values in the columns
for gid, indices in tqdm(gid2Indices.items()):
    if len(indices) == 0:
        continue
    bestIndex = None
    bestCount = -1
    for index in indices:
        count = nonNullFeaturesCount[index]
        if count > bestCount:
            bestCount = count
            bestIndex = index
    if bestIndex is not None:
        gid2BestIndex[gid] = bestIndex



columns = [
    'grant_agency_normalized',
    'ids:ror',
    'types',
    'ancestorTypes',
    'oa_ancestortypes',
    'in_dimensions',
    'sid',
    'jin_type',
    'comb_id',
    'gvkey',
    'fyear',
    'fyr',
    'datadate',
    'name',
    'cik',
    'sic',
    'spcindcd',
    'spcseccd',
    'spcsrc',
    'regions',
    'type',
    'isin',
    'EIN',
    'LEVEL1',
    'FNDNCD',
    'FIPS',
    'TAXPER',
    'URL',
    'gid',
    'funder_country_code',
]

mergedData = {}
for column in columns:
    mergedData[column] = [None]*len(dfGrantAgency)

for index, gid in enumerate(tqdm(dfGrantAgency['gid'])):
    if gid is None or gid not in gid2BestIndex:
        continue
    bestIndex = gid2BestIndex[gid]
    for column in columns:
        mergedData[column][index] = dfGrantMatchesWithCounts.at[bestIndex, column]

for column in columns:
    if(column not in dfGrantAgency):
        dfGrantAgency[column] = mergedData[column]
    else:
        dfGrantAgency[column+"_matched"] = mergedData[column]



# add column for affiliation_country_codes split by ;
dfGrantAgency['affiliation_country_codes'] = dfGrantAgency['UT'].map(lambda x: ";".join(uid2CountryCode.get(x, [])))

dfGrantAgency.to_feather(papersMatchPath)
dfGrantMatchesWithCounts.to_feather(agenciesMatchPath)

# dfGrantMatchesWithCounts = pd.read_feather(outputPath / "Results/grant_agency_matched_jin_2023_affiliation_match_v3.feather")
# save as parquet
dfGrantAgency.to_parquet(papersMatchPathParquet)
dfGrantMatchesWithCounts.to_parquet(agenciesMatchPathParquet)


# dfGrantAgency.to_feather(outputPath / "Results/institutions_jin_2023_affiliation_match_v3.feather")
# dfGrantMatchesWithCounts.to_feather(outputPath / "Results/grant_agency_matched_jin_2023_affiliation_match_v3.feather")

# # dfGrantMatchesWithCounts = pd.read_feather(outputPath / "Results/grant_agency_matched_jin_2023_affiliation_match_v3.feather")
# # save as parquet
# dfGrantAgency.to_parquet(outputPath / "Results/institutions_jin_2023_affiliation_match_v3.parquet")
# dfGrantMatchesWithCounts.to_parquet(outputPath / "Results/grant_agency_matched_jin_2023_affiliation_match_v3.parquet")


