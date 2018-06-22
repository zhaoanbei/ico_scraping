
# coding: utf-8

# In[81]:


'''
basic steps:
1(c)Get all post [22k]
2(c)filter out posts without urls[22k]
3(c)filter out posts in certain categories(bounty, $$$how many cates?)[20k]
(c)add: move airdrops somewhere else
4filter out pure ip($$$reg)
5filter out http:, #post($$$reg)
6(c)filter out complex urls
7(c)filter out social/platform/forum links
8(c)make domain, domainlist and domain length
9(c)filter out frequent domains
10(c)find names
11(c)→1 match names with domains[6279]
12(c)→2 find domains do not match but happen once in all posts and only one in certain post
13problem: posts without official websites: $$$ check 
14(c)filter out sub-domains(split by .)
15(c)→3 find domains hanppen once
16(c)→4 higher frequency, find the most possible urls
17check in excel
'''


import warnings; warnings.simplefilter('ignore')
#1获取data 
import pandas as pd
data = pd.read_json('0606final.json')
data = data[['pageUrl', 'text', 'links', 'name_opt1', 'name_opt2', 'recommendedWebsite']]

#2有url
data = data[data['links'].apply(lambda x: len(x)) > 0]

#3去除(bounty, airdrop)20369
cate_filter_out = ['bounty', 'airdrop']
data_ad = data[data['text'].str.contains('airdrop')]#1548
data = data[~ data['text'].str.contains('|'.join(cate_filter_out))]

#6简化url
def short_url(li):
    new_li = []
    for i in range(len(li)):
        url = li[i]['url'] #string
        dom = url.split('/') #list
        if len(dom)<4:
            new_dic = {}
            new_dic['url'] = url
            new_dic['text'] = li[i]['text']
            new_li.append(new_dic)
    return new_li

def combine(dictionaries):
    combined_dict = {}
    if len(dictionaries)>0:
        for dictionary in dictionaries:
            for key, value in dictionary.items():
                combined_dict.setdefault(key, []).append(value)
    else:
        combined_dict['url']=[]
    return combined_dict
def get_url(dic):
    return dic['url']

data['short_url'] = data.links.map(short_url).map(combine).map(get_url)

#7去除无用domain
li_not_in = ['twitter','facebook', '.pdf','github.com','t.me','telegram','youtu','weibo','wechat',      'bitcointalk','bitsharestalk','instagram','cryptocointalk','reddit','coinlib','medium',     'linkedin','goo','discord','slack','@','mega','imgur','steemit','etherscan','blockchain.info',     'wallet','bit.ly','trello','tinypic','icobench','whatsapp','huobi','dropbox','pool','yobit',      'amazon','tumblr','webchat','forum','blog','suspicious','[img]','yahoo','.png','.jpg']
def not_in(lst):
    new_lst = []
    for i in lst:
        new_lst = list(s for s in lst if not any(e in s for e in li_not_in))
    return new_lst

data['no_public_links'] = data.short_url.map(not_in)


# In[82]:


#8去除无用网址后的domain和list
def domain(st):
    dom = st.split('/')
    if len(dom)>2:
        return(dom[2])
    return dom[-1]

def key(li):
    key_obj = {}    
    for i in range(len(li)):
        dom = domain(li[i]).replace('www.','')
        if len(dom)>0:
            if dom not in key_obj: 
                key_obj[dom] = []
            key_obj[dom].append(li[i])

    return key_obj

data['domain'] = data.no_public_links.map(key)#no_public_links

def dic_li(dic):
    return list(dic.keys())

data['domLst'] = data.domain.map(dic_li)
data['domLen'] = data.domLst.map(len)

#根据ico名字得到dom(domLstN)
data['dom_name'] = data.recommendedWebsite.map(key)
data['domLstN'] = data.dom_name.map(dic_li)


#11两个list匹配 (name&no_public)
def drop_dup(lst):
    return list(set(lst))
data.domLstN = data.domLstN.map(drop_dup)
data['domLenN'] = data.domLstN.map(len)
data['name&no_public']=data.domLstN + data.domLst
def check_dup(l):
    return set([x for x in l if l.count(x) > 1])
data['name&no_public'] = data['name&no_public'].map(check_dup)
data['name&no_public_Len'] = data['name&no_public'].map(len)
data = data.sort_values(by=['name&no_public_Len'], ascending=False )

#第一部分：ico名字与去除无关网址后匹配 'name&no_public'
m1 = data.loc[data['name&no_public_Len'] ==1]#4999 正好是1
def fst(se):
    return se.pop()
m1['name&no_public']=m1['name&no_public'].map(fst)#set变string
m1 = m1.rename(columns={'name&no_public': 'dom'})#选三列
m1 = m1[['pageUrl','text','dom']]

def clean(st):
    head, sep, tail = st.partition(':')
    head, sep, tail = head.partition('?')
    head, sep, tail = head.partition('(')
    head, sep, tail = head.partition('[')
    head, sep, tail = head.partition('"')
    head, sep, tail = head.partition('#')
    if len(head)<6:
        return ''
    else:
        return head
m1.dom = m1.dom.map(clean)#去subdom

m1m1 = data.loc[data['name&no_public_Len'] >1]#1423有多个解

# 可能的后缀
li = [ 'io', 'com','net','org','pw','cc','cn','eu','info','pm','pro','de','global',      'asia','xyz','online','site','one','cash','explorer','aero','ngo','in','us','uk',     'co','science','top','world','tech','life','ai','space','me','club','pl','market','social',     'fi','is','uz','ae','ec','official','systems','ad']
def houzhui(lst):
    new_lst = []
    for i in lst:
        new_lst = list(s for s in lst if any(e in s for e in li))
    return new_lst

m1m1['houzhui'] = m1m1['name&no_public'].map(houzhui)
m1m1['houzhuiL'] = m1m1['houzhui'].map(len)

#最短
def short_st(lst):
    if len(lst)>0:
        return min(lst, key=len)
    else:
        return ''

m1m1['houzhuishort'] = m1m1['houzhui'].map(short_st)#注意：这里取了最短dom
m1m1['houzhuishortEpt'] = m1m1['houzhuishort'].map(len)
m1m1 = m1m1.rename(columns={'houzhuishort': 'dom'})#选三列
m1m1 = m1m1[['pageUrl','text','dom']]
m1m1.dom = m1m1.dom.map(clean)#去subdom
m1c = pd.concat([m1,m1m1])

# 一些混蛋 icos
m1c.loc[m1c.pageUrl == 'https://bitcointalk.org/index.php?topic=3041802.0', 'dom'] = "rareshares.tk"


# In[83]:


def which_is_ip(st):
    if st.isupper() or st.islower():
        return 'valid'
    elif len(st)>1:
        return 'ip'
    else:
        return 'empty'
m1c['typ'] = m1c.dom.map(which_is_ip)
m1c.loc[m1c.typ == 'ip']#1
m1c.loc[m1c.typ == 'empty']#7
m1c.loc[m1c.typ == 'valid']#6256

m1c.to_csv('partI.csv')


# In[84]:


#part2 出现频率 

m10 = data.loc[data['name&no_public_Len'] ==0]#14091

#频率表
lis = []
for k in range(len(m10)):
    for i in m10.iloc[k,-4]:
        lis.append(i)
        
counts = pd.Series(lis).value_counts()
counts_df = pd.DataFrame({'name':counts.index, 'value':counts.values})
counts_df


#读取频率表
#counts_df = pd.read_csv('frequency.csv', header=None)
#3次
counts = counts_df.loc[counts_df['value'] <3]
counts_li = counts['name'].tolist()

def find_les_fre(lst):
    return list(s for s in lst if any(e in s for e in counts_li))

m10['dom_a'] = m10['domLst']+m10['domLstN']
m10['no_ire'] = m10.dom_a.map(not_in)

m10['fre3']=m10.no_ire.map(find_les_fre)
m10[['pageUrl','text','fre3']].head()


# In[85]:


#出现1次（fre3)
m10['fre3l']=m10.fre3.map(len)
mP2Fre3c = m10.loc[m10.fre3l==1]
mP2Fre3c.fre3 = mP2Fre3c.fre3.map(fst)
mP2Fre3c.fre3 = mP2Fre3c.fre3.map(clean)
mP2Fre3c = mP2Fre3c[['pageUrl','text','domLst','domLstN','fre3']]
mP2Fre3c['typ'] = mP2Fre3c.fre3.map(which_is_ip)
mP2Fre3c.to_csv('partII1.csv')


# In[86]:


#出现1次以上(fre3)
m10_frem1 = m10[['pageUrl','text','domLst','domLstN','fre3','fre3l']]
mP2Fre3m = m10_frem1.loc[m10_frem1.fre3l>1]
mP2Fre3m #994


# In[91]:


# part 3 
only1urlaftercleanirre = m10.loc[m10.domLen==1].reset_index()
only1urlaftercleanirre = only1urlaftercleanirre[['pageUrl','text','domLst']]

for i in range(len(only1urlaftercleanirre)):
    only1urlaftercleanirre['domLst'][i] = only1urlaftercleanirre.domLst[i][0]
    
only1urlaftercleanirre['domLst'] = only1urlaftercleanirre['domLst'].map(clean)
only1urlaftercleanirre['typ'] = only1urlaftercleanirre.domLst.map(which_is_ip)

#tiny changes
'''only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl ==                            'https://bitcointalk.org/index.php?topic=517339.0',                            'domLst'] = "vcoin.ca"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=517261.0', \
                           'domLst'] = "vcoin.ca"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=516183.0', \
                           'domLst'] = "vcoin.ca"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=515483.0', \
                           'domLst'] = "vcoin.ca"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=562027.0', \
                           'domLst'] = ""
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=667420.0', \
                           'domLst'] = "buybackcoin.net"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=2503702.0', \
                           'domLst'] = ""
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=528173.0', \
                           'domLst'] = ""
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=771292.0', \
                           'domLst'] = "dbcoin.info"
only1urlaftercleanirre.loc[only1urlaftercleanirre.pageUrl == \
                           'https://bitcointalk.org/index.php?topic=1281316.0', \
                           'domLst'] = ""'''

only1urlaftercleanirre.to_csv('only1urlaftercleanirre.csv')

