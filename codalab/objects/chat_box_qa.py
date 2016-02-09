'''
ChatBoxQA is a class that automatically answers users' questions or comments they put in the chat box.
It returns the question it is trying to answer, its answer and the recommended command to run in the terminal
if it is confident about what the user is asking, otherwise returns None
'''

import yaml
import re
import string
import os

class ChatBoxQA(object):
	qa_body = {}
	@classmethod
	def prepare(cls):
		file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'chat_box_qa.yaml')
		with open(file_path, 'r') as stream:
			cls.qa_body = yaml.load(stream)

	@classmethod
	def get_similarity(cls, query_question, model_question):
		exclude = set(string.punctuation)
		query_tokens = set(''.join(ch for ch in query_question.lower() if ch not in exclude).split())
		model_tokens = set(''.join(ch for ch in model_question.lower() if ch not in exclude).split())
		count = 0
		for query_token in query_tokens:
			for model_token in model_tokens:
				if query_token == model_token:
						count += 1
 		union_cardinality = len(query_tokens.union(model_tokens))
 		return count / float(union_cardinality)

 	@classmethod
 	def get_custom_params(cls, question):
 		if 'this' in question:
 			if 'worksheet' in question:
 				return 'worksheet'
 			elif 'bundle' in question:
 				return 'bundle'
 		return None

 	@classmethod
	def get_most_similar_question_index(cls, question):
		highest_sim = float("-inf")
		highest_idx = 0
		for index in cls.qa_body:
			current_sim = cls.get_similarity(question, cls.qa_body.get(index)['question'])
			if current_sim > highest_sim:
				highest_sim = current_sim
				highest_idx = index
		if highest_sim < 0.5:
			highest_idx = None
		return (highest_idx, cls.get_custom_params(question))
 
	@classmethod
	def answer(cls, question, worksheet_uuid, bundle_uuid):
		cls.prepare()
		index, custom_params = cls.get_most_similar_question_index(question)
		if index == None:
			return None
		question = cls.qa_body.get(index)['question']
		response = cls.qa_body.get(index)['answer']['response']
		if custom_params == 'worksheet':
			command = ' '.join(cls.qa_body.get(index)['answer']['command'].split()[:2] + [worksheet_uuid])
		elif custom_params == 'bundle' and bundle_uuid != '-1':
			command = ' '.join(cls.qa_body.get(index)['answer']['command'].split()[:2] + [bundle_uuid])
		else:
			command = cls.qa_body.get(index)['answer']['command']
		return (question, response, command)