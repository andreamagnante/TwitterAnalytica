import streamlit as st
import numpy as np
import pandas as pd
import json, pprint, requests, textwrap,time
import matplotlib.pyplot as plt
import spark_twitter

st.title('Twitter Analytica')

# Possiamo passare qualsiasi cosa alla funzione st.write (Matplot figures, testo ecc)

st.write("L'app streamlit comunica con il nostro cluster EMR:")

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def remote_css(url):
    st.markdown(f'<link href="{url}" rel="stylesheet">', unsafe_allow_html=True)    

def icon(icon_name):
    st.markdown(f'<i class="material-icons">{icon_name}</i>', unsafe_allow_html=True)

local_css("style.css")
remote_css('https://fonts.googleapis.com/icon?family=Material+Icons')

icon("search")

interval = st.selectbox('Which interval do you want to monitor?', ['Last 24h', 'Last 3 days', 'Last week'], 0)  # default value = 'Last 24h'

print(interval)

selected = st.text_input("")
button_clicked = st.button("OK")

# IL CODICE DEL WORD COUNT SI TROVA SUL NOTEBOOK JUPYTER
def clean_text(text):
	text = text.replace('[', '')
	text = text.replace(']', '')
	text = text.replace('{', '')
	text = text.replace('}', '')
	text = text.replace("\\", "")
	text = text.replace("'", "")
	text = text.replace("\"", "")
	return text

if (button_clicked):
	# Qui faccio partire una funzione che mi ricalcola il nuovo df con le statistiche spark
	tweets = spark_twitter.spark_search(selected, interval)
	del tweets[0]

	if len(tweets) == 0:
		t = "<div><span class='highlight red'> <span class='bold'>Nessun tweet collegato all'argomento</span></span></div>"
		st.markdown(t, unsafe_allow_html=True)
		#st.write("Nessun tweet collegato all'argomento")

	else:
		for x in tweets:
			x = clean_text(x)
			st.write(x)

		# Altre 2 funzioni di statistiche
		retweets = spark_twitter.count_retweet(tweets)
		tags = spark_twitter.count_tags(tweets)

		if retweets != 0:
			t = "<div><span class='highlight blue'><span class='bold'>Numero retweets relativi all'argomento: "+ str(retweets) +" </span></span></div>"
			st.markdown(t, unsafe_allow_html=True)

		if len(tags) > 0:
			df_tags = pd.DataFrame(tags, columns =['Tags','occurences'])
			df_tags = df_tags.rename(columns={'Tags':'index'}).set_index('index') # Setto la colonna hashtag come indice per visualizzarla su xaxis

			df_tags_limit = df_tags.head(20) 	
			st.dataframe(df_tags_limit)

			chat_data_tags = pd.DataFrame(df_tags_limit, columns=['Tags', 'occurences'])
			st.bar_chart(chat_data_tags)

		dict_result = spark_twitter.word_count(tweets)

		#print(len(dict_result))

		if len(dict_result) > 0: 
			df = pd.DataFrame(dict_result, columns =['Hashtag','occurences'])
			df = df.rename(columns={'Hashtag':'index'}).set_index('index') # Setto la colonna hashtag come indice per visualizzarla su xaxis
		  
			st.dataframe(df)

			df_limit = df.head(10)
			#st.dataframe(df_limit)

			chart_data = pd.DataFrame(df_limit, columns =['Hashtag','occurences'])

			st.bar_chart(chart_data)

		else: 
			t = "<div><span class='highlight blue'> <span class='bold'>Nessun hashtag collegato all'argomento</span></span></div>"
			st.markdown(t, unsafe_allow_html=True)
