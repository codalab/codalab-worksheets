"""
Chatbox API
"""
import os

from bottle import get, local, post, request
import yaml

from codalab.objects.chat_box_qa import ChatBoxQA
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/chats', apply=AuthenticatedPlugin())
def get_chat_box():
    """
    Return a list of chats that the current user has had
    """
    query = {
        'user_id': request.user.user_id,
    }
    return {
        'chats': local.model.get_chat_log_info(query),
        'root_user_id': local.model.root_user_id,
        'system_user_id': local.model.system_user_id,
    }


@post('/chats', apply=AuthenticatedPlugin())
def post_chat_box():
    """
    Add the chat to the log.
    Return an auto response, if the chat is directed to the system.
    Otherwise, return an updated chat list of the sender.
    """
    recipient_user_id = request.POST.get('recipientUserId', None)
    message = request.POST.get('message', None)
    worksheet_uuid = request.POST.get('worksheetId', -1)
    bundle_uuid = request.POST.get('bundleId', -1)
    info = {
        'sender_user_id': request.user.user_id,
        'recipient_user_id': recipient_user_id,
        'message': message,
        'worksheet_uuid': worksheet_uuid,
        'bundle_uuid': bundle_uuid,
    }
    chats = add_chat_log_info(info)
    return {'chats': chats}


# @get('/faqs')
def get_faq():
    """
    Return a list of FAQ items, each of the following format:
    '0': {
        'question': 'how can I upload / add a bundle?'
        'answer': {
            'response': 'You can do cl upload or click Update Bundle.',
            'command': 'cl upload <file_path>'
        }
    }
    Currently disabled. Needs further work.
    """
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/chat_box_qa.yaml')
    with open(file_path, 'r') as stream:
        content = yaml.load(stream)
        return {'faq': content}


def add_chat_log_info(query_info):
    """
    Add the given chat into the database.
    |query_info| encapsulates all the information of one chat
    Example: query_info = {
        'sender_user_id': 1,
        'recipient_user_id': 2,
        'message': 'Hello this is my message',
        'worksheet_uuid': 0x508cf51e546742beba97ed9a69329838,   // the worksheet the user is browsing when he/she sends this message
        'bundle_uuid': 0x8e66b11ecbda42e2a1f544627acf1418,   // the bundle the user is browsing when he/she sends this message
    }
    Return an auto response, if the chat is directed to the system.
    Otherwise, return an updated chat list of the sender.
    """
    updated_data = local.model.add_chat_log_info(query_info)
    if query_info.get('recipient_user_id') != local.model.system_user_id:
        return updated_data
    else:
        message = query_info.get('message')
        worksheet_uuid = query_info.get('worksheet_uuid')
        bundle_uuid = query_info.get('bundle_uuid')
        bot_response = format_message_response(ChatBoxQA.answer(message, worksheet_uuid, bundle_uuid))
        info = {
            'sender_user_id': local.model.system_user_id,
            'recipient_user_id': request.user.user_id,
            'message': bot_response,
            'worksheet_uuid': worksheet_uuid,
            'bundle_uuid': bundle_uuid,
        }
        local.model.add_chat_log_info(info)
        return bot_response


def format_message_response(params):
    """
    Format automatic response
    |params| is None if the system can't process the user's message
    or is not confident enough to give a response.
    Otherwise, |params| is a triple that consists of
    the question that the system is trying to answer,
    the response it has for that question, and the recommended command to run.
    Return the automatic response that will be sent back to the user's chat box.
    """
    if params is None:
        return 'Thank you for your question. Our staff will get back to you as soon as we can.'
    else:
        question, response, command = params
        result = 'This is the question we are trying to answer: ' + question + '\n'
        result += response + '\n'
        result += 'You can try to run the following command: \n'
        result += command
        return result

