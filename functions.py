import pandas as pd
import re
from bs4 import BeautifulSoup
from curl_cffi import requests as cureq
from datetime import datetime, timedelta

# Load a CSV file into a pandas DataFrame.
def load_data(filepath):
    return pd.read_csv(filepath)
    
# sending request to stepstone server, returning response as soup
def scrape_stepstone(job_title, page, language, worktime, sector):
    url = f'https://www.stepstone.de/jobs/{worktime}/{job_title}/in-berlin?radius=30&whereType=autosuggest&page={page}action=facet_selected%3bworktypes%3b80001&fdl={language}&se={sector}&wci=419239&sort=1&action=sort_relevance'

    # print the url for reference check
    #print('---------------------------------')
    #print(url)
    #print('---------------------------------')
    
    response = cureq.get(url, impersonate='chrome')
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

#  scraping the result, returning dataframe
def enrich_dataset(job_title, page, language, worktime, sector, dataframe):
    df_base = dataframe
    df_enrich = dataframe.drop(dataframe.index)
    
    # scrape the initial result via scrape function
    scrape_result = scrape_stepstone(job_title, page, language, worktime, sector)
    
    # get all job listings
    job_offer_list = scrape_result.find_all(['article'], attrs={'class': 'res-1p8f8en'})

    # set language, job_type, sector, source, search term according to search parameters
    job_type_list = []
    language_list = []
    sector_list = []
    source_list = []
    search_term_list = []
    for job_offer in job_offer_list:
        job_type_list.append((lambda worktime: 'Full-time' if worktime == 'vollzeit' else 'Part-time' if worktime == 'teilzeit' else 'Unknown')(worktime))
        language_list.append(language)
        sector_list.append((lambda sector: 'IT Services and IT Consulting' if sector == '21000' else 'Business Consulting and Services' if sector == '23000' else 'Retail' if sector == '15000' else 'Finance' if sector == '19001' or sector == '19002' else 'Unknown')(sector))
        source_list.append('stepstone')
        search_term_list.append(job_title.replace('-', ' '))
        
    # scrape job titles
    job_title_list = []
    for job_offer in job_offer_list:
        job_title_list.append(job_offer.find(['div'], attrs={'class': 'res-nehv70'}).get_text())
    
    # scrape company names
    company_name_list = []
    for job_offer in job_offer_list:
        company_name_list.append(job_offer.find(['span'], attrs={'class': 'res-btchsq'}).get_text())
    
    # scrape post date
    post_date_list = []
    for job_offer in job_offer_list:
        post_date_list.append(handle_date(job_offer.find(['time']).get_text()))
        
    # scrape and wrangle remote 
    job_remote_list = []
    for job_offer in job_offer_list:
        result = job_offer.find_all(['div'], attrs={'class': 'res-lgmafx'})
        remote = result[0].find(['span'], attrs={'class': 'res-1qh7elo'})
        if not remote:
            job_remote_list.append('On-site')
        else:
            remote_text = remote.find(['span'], attrs={'class': 'res-btchsq'}).get_text()
            if remote_text == 'Teilweise Home-Office':
                job_remote_list.append('Hybrid')
            elif remote_text == 'Nur Home-Office':
                job_remote_list.append('Remote')
            else:
                job_remote_list.append('On-site')
    
    # fill the dataframe with the list values
    df_enrich['job_title'] = job_title_list
    df_enrich['company_name'] = company_name_list
    df_enrich['post_date'] = post_date_list
    df_enrich['job_type'] = job_type_list
    df_enrich['job_remote'] = job_remote_list
    df_enrich['language'] = language_list
    df_enrich['sector'] = sector_list
    df_enrich['source'] = source_list
    df_enrich['search_term'] = search_term_list

    # combine base dataframe with the enriching data
    df_combined = pd.concat([df_base, df_enrich],axis=0)
    return df_combined

# execute the scrape function in a nested loop to scrape a lot of data
def scrape_a_lot(df_base):
    # define the search parameters for the iterative scraping
    search_terms = ['data analyst', 'data engineer', 'data scientist']
    languages = ['en', 'de']
    worktimes = ['vollzeit', 'teilzeit']
    #21000 for 'it & internet', 23000 for 'bwl/business' 15000 for 'retail', 19001 for 'bank', 19002 for 'finance'  
    sectors = ['21000', '23000', '15000', '19002']
    df_temp = df_base.drop(df_base.index)
    
    # !!!BE CAREFUL - DO NOT UNCOMMENT AND EXECUTE!!!
    # this will cause a lot of requests in short time and might result in an IP ban
    #for language in languages:
    #    for worktime in worktimes:
    #        for search in search_terms:
    #            for sector in sectors:
    #                for i in range(1,3):
    #                    df_temp = enrich_dataset(search, str(i), language, worktime, sector, df_base)
    return df_temp

# function to handle date format like "1 week ago" and convert it to datetime format
def handle_date(stepstone_post_date):
    sliced = stepstone_post_date.split(' ', 2)
    date_number = int(sliced[1])
    time_format = sliced[2]
    date = datetime.now()
    
    match time_format:
        case 'Stunden' | 'Stunde':
            date = date - timedelta(hours=date_number)
        case 'Tagen' | 'Tag':
            date = date - timedelta(days=date_number)
        case 'Wochen' | 'Woche':
            date = date - timedelta(weeks=date_number)
        case 'Monate' | 'Monat':
            date = date - timedelta(months=date_number)
        case _:
           date = date
    return date.strftime('%d-%m-%Y')

# job level mapping
def replace_nan_with_job_level(row):
    job_level_mapping = {
        'Senior': 'Senior Level',
        'Mid-Senior level': 'Senior Level',
        'Associate': 'Entry Level',
        'Entry': 'Entry Level',
        'Entry level': 'Entry Level',
        'Werkstudent': 'Entry Level',
        'Work student': 'Entry Level',
        'Working student': 'Entry Level',
        'Praktikum' :'Entry Level',
        'Junior': 'Entry Level',
        'Graduate': 'Entry Level',
        'Sr.': 'Senior Level',
        'Consultant': 'Director Level',
        'Developer': 'Senior Level',
        'Mid': 'Senior Level',
        'Intern': 'Entry Level',
        'Lead': 'Senior Level',
        'Manager': 'Director Level',
        'Internship': 'Entry Level',
        'Director': 'Director Level',
        'Head':'Director Level',
        'Mid-Senior': 'Senior Level',
        'Werkstudententätigkeit': 'Entry Level',
        'Student': 'Entry Level',
        'Sr.': 'Senior Level',
        'Leiter': 'Senior Level',
        'Principal': 'Senior Level',
        'Senior': 'Senior Level',
        'Head': 'Director Level',
        'Chief': 'Director Level',
        'Officer': 'Director Level',
    }
    if pd.isna(row['job_level']):
        for keyword, mapping in job_level_mapping.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', row['job_title'], re.IGNORECASE):
                return mapping
        return 'Senior Level' 
    else:
        for keyword, mapping in job_level_mapping.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', row['job_level'], re.IGNORECASE):
                return mapping

# sector mapping
def replace_sectors(row):
    sector_mapping = {
    'Utilities': 'Energy',
    'Banking': 'Finance',
    'Retail': 'Commerce',
    'IT Services and IT Consulting': 'Technology',
    'Software Development': 'Technology',
    'Hospitals and Health Care': 'Healthcare',
    'Telecommunications': 'Communication',
    'Automotive': 'Automotive',
    'Manufacturing': 'Industry',
    'Real Estate': 'Construction',
    'Pharmaceutical Manufacturing': 'Healthcare',
    'Food & Beverages': 'Food Industry',
    'E-Learning Providers': 'Education',
    'Advertising Services': 'Marketing',
    'Automation Machinery Manufacturing': 'Industry',
    'Retail Apparel and Fashion': 'Commerce',
    'Computer Games': 'Entertainment',
    'Technology, Information and Internet': 'Technology',
    'Financial Services': 'Finance',
    'Engineering Services': 'Engineering',
    'Appliances, Electrical, and Electronics Manufacturing': 'Industry',
    'Information Technology & Services': 'Technology',
    'Staffing and Recruiting': 'Human Resources',
    'Human Resources Services': 'Human Resources',
    'Executive Offices': 'Administration',
    'Broadcast Media Production and Distribution': 'Media',
    'Online and Mail Order Retail': 'Commerce',
    'Computer and Network Security': 'Technology',
    'Business Consulting and Services': 'Business',
    'Human Resources': 'Human Resources',
    'Travel Arrangements': 'Travel',
    'Rail Transportation': 'Transportation',
    'Security and Investigations': 'Security',
    'Think Tanks': 'Research',
    'Entertainment Providers': 'Entertainment',
    'Investment Banking': 'Finance',
    'Truck Transportation': 'Transportation',
    'Marketing Services': 'Marketing',
    'Medical Equipment Manufacturing': 'Healthcare',
    'Musicians': 'Entertainment',
    'Renewable Energy Semiconductor Manufacturing': 'Energy',
    'Professional Services': 'Consulting',
    'Consumer Services': 'Customer Service',
    'Farming': 'Agriculture',
    'Hospitality': 'Hospitality',
    'Higher Education': 'Education',
    'Research Services': 'Research',
    'Education': 'Education',
    'Law Practice': 'Legal',
    'Book and Periodical Publishing': 'Publishing',
    'Graphic Design': 'Design',
    'Mobile Gaming Apps': 'Entertainment',
    'Machinery Manufacturing': 'Industry',
    'Semiconductor Manufacturing': 'Technology',
    'Venture Capital and Private Equity Principals': 'Finance',
    'Professional Training and Coaching': 'Education',
    'Insurance': 'Finance',
    'Ground Passenger Transportation': 'Transportation',
    'Wholesale': 'Commerce',
    'Textile Manufacturing': 'Industry',
    'Technology, Information and Media': 'Technology',
    'Government Administration': 'Government',
    'Non-profit Organizations': 'Non-Profit',
    'Public Safety': 'Safety',
    'Civil Engineering': 'Engineering',
    'Personal Care Product Manufacturing': 'Consumer Goods',
    'Accounting': 'Finance',
    'Biotechnology': 'Healthcare',
    'Information Services': 'Technology',
    'Legal Services': 'Legal',
    'Cosmetics': 'Consumer Goods',
    'Internet Publishing': 'Publishing',
    'Finance': 'Finance'
    }
    if pd.isna(row['sector']):
        return 'Unknown' 
    else:
        for keyword, mapping in sector_mapping.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', row['sector'], re.IGNORECASE):
                return mapping
        return 'Unknown'