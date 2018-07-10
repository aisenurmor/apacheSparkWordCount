
# coding: utf-8

# In[1]:


import findspark


# In[2]:


findspark.init('/home/aisenur/apache-spark')


# In[3]:


from pyspark import SparkContext, SparkConf


# In[4]:


sc=SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


# In[5]:


metin=sc.textFile("/home/aisenur/text.txt")


# In[6]:


metin.count()


# In[7]:


kelime=metin.flatMap(lambda satir: satir.split(" "))


# In[8]:


tuples=kelime.map(lambda x: (x,1))


# In[9]:


counts=tuples.reduceByKey(lambda a,b: (a+b))


# In[10]:


output=counts.collect()
for(word,count) in output:
    print ("%s: %i" %(word, count))

