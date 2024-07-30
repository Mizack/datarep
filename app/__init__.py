import logging
import os

current_path = os.path.dirname(__file__)
parent_path = os.path.dirname(current_path)

templates_path = os.path.join(parent_path, 'logs')
if not os.path.exists(templates_path):
    os.makedirs(templates_path)

logging.basicConfig(filename='logs/LOGS_REQUISICAO.log', encoding='utf-8')