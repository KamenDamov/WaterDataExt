#Run script
import schedule as sch
import time

def run_script(): 
    print('Extraction start')
    import requests
    from bs4 import BeautifulSoup as bs 
    import pandas as pd
    import numpy as np
    from os import environ
    import os
    from textblob import TextBlob
    import schedule as sch
    import time

    #Scrape data
    url='https://hydro.bg/bg/t1.php?ime=&gr=data/&gn=tablRekiB2017'
    page = requests.get(url)
    data=page.content
    soup = bs(data, 'html.parser')
    titleHeader=[]
    for element in soup.select('h2'):
        titleHeader.append(element.text.strip())

    print(titleHeader)
    #header title
    addTitle = ' '.join(titleHeader[0].split(' ')[6:7])
    

    #Get headers 
    dataHeader=[]
    for element in soup.select('h3'):
        dataHeader.append(element.text.strip())

    #Get content of the rows in tnhe tables
    dataContent=[]
    for element in soup.select('tr'):
        dataContent.append(element.text.strip())

    j = 0
    fullData = []
    header = []
    for i in range(len(dataContent)):
        #Cut off data in a different table, the marker is the characters below
        data = []
        if dataContent[i-1][:5] == '№ ХМС':
            j = i
            header.append(dataContent[i-1])
            #Continue appending while in the same table
            while dataContent[j][:5] != '№ ХМС':
                data.append(dataContent[j])
                j += 1 
                if j == len(dataContent): 
                    break
            fullData.append(data)

    #fullData
    final_arr = []
    
    #Creating final arrays based on string data extracted above
    for i in range(len(fullData)):
        inner_arr = []
        for j in range(len(fullData[i])): 
            inner_arr.append(fullData[i][j].split('\n'))
        final_arr.append(inner_arr)

    header = header[0].split('\n')[::2]
    dataHeader = dataHeader[:-1]

    #Create dataframes 
    dfs = []
    for i in range(len(final_arr)): 
        df = pd.DataFrame(final_arr[i], columns=header)
        index = df.index
        index.name = dataHeader[i]
        dfs.append(df)
    counter=0

    #Implement for loop to clean commas and add dots
    for df in dfs: 
        trans_cols = []
        print(df.columns) 
        to_verify = df.columns[3:]
        counter+=1
        for index, rows in df.iterrows():
            #print(to_translate)
            for i in to_verify: 
                if rows[i] is None: 
                    continue
                elif 'n.a.' in rows[i]: 
                    rows[i] = '-999.9'
                rows[i] = rows[i].replace(',','.')
                rows[i] = rows[i].replace(' ','')
            '''
            for k in to_translate:
                try: 
                    bg_blob = TextBlob(rows[k])
                    rows[k] = bg_blob.translate(from_lang='bg', to='en')
                except: 
                    pass
            '''
        for j in to_verify:
            try:
                df[to_verify].astype(float)
            except: 
                pass

    for index, rows in dfs[3].iterrows(): 
        if '.' in rows['№ ХМС']: 
            dfs[3] = dfs[3][:index]
    
    #Add a date column
    for df in dfs: 
        df['Date'] = addTitle
    
    for df in dfs:
        df.rename({
        '№ ХМС':'No. HMS',
        'Река': 'River',
        'Хидрометрична станция (ХМС)': 'Hydrometric Station (HMS)'
    }, axis= 1, inplace = True)
    
    #Check if paths exist (to have for the first extraction)
    path_tab = ['../tools/bassins/Дунавски басейн.csv',\
                '../tools/bassins/Черноморски басейн.csv',\
                '../tools/bassins/Източнобеломорски басейн.csv',\
                '../tools/bassins/Западнобеломорски басейн.csv']

    for i in range(len(path_tab)): 
        if os.path.exists(path_tab[i]): 
            print('Path exists')
            #Import data we have at the moment
            df_append = pd.read_csv(path_tab[i])
            if df_append['Date'][0] == dfs[i]['Date'][0]:
                print('Same date detected')
                continue
            else:
                dfs[i] = pd.concat([dfs[i], df_append], ignore_index=True)
                dfs[i].to_csv('../tools/bassins/'+dataHeader[i]+'.csv',encoding='utf-8-sig')
        else:
            print('Path does not exist yet')
            newpath = r'../tools/bassins'
            if not os.path.exists(newpath):
                os.makedirs(newpath)
            dfs[i].to_csv('../tools/bassins/'+dataHeader[i]+'.csv',encoding='utf-8-sig')
    
    print('Done!')

#Run the Scrip
sch.every().day.at('05:00').do(run_script)
while True:
    sch.run_pending()
    time.sleep(1)
