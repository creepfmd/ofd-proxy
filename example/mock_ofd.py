# coding: utf8
#
#        Copyright (C) 2017 Yandex LLC
#        http://yandex.com
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#

import asyncio
import datetime
from elasticsearch import Elasticsearch
import argparse
import socket
from ofd.protocol import SessionHeader, FrameHeader, unpack_container_message

ES_URL = "localhost"
OFD_URL = "ofdt.platformaofd.ru"
OFD_PORT = 19081

async def handle_connection(rd, wr):
    """
    Пример использования протокола для эмуляции работы ОФД. Сервер принимает входящее сообщение и распаковывает его,
    выводя значения в stdout. В ответ сервер формирует сообщение "подтверждение оператора" и передает его обратно кассе.
    Эмулятор работает без использования шифровальный машины, поэтому считаем, что сообщение приходит в ОФД
    в незашифрованном виде.
    :param rd: readable stream.
    :param wr: writable stream.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((OFD_URL, OFD_PORT))

        # Разбираем входящее сообщение
        session_raw = await rd.readexactly(SessionHeader.STRUCT.size)
        session = SessionHeader.unpack_from(session_raw)
        print(session)
        container_raw = await rd.readexactly(session.length)

        # Входящее сообщение ККТ
        header_raw, message_raw = container_raw[:FrameHeader.STRUCT.size], container_raw[FrameHeader.STRUCT.size:]
        doc = unpack_container_message(message_raw, b'0')[0]
        # print(json.dumps(doc, ensure_ascii=False, indent=4))
        # Если тип документа 1 (продажа), то укладываем в эластик
        if 'receipt' in doc:
            if doc['receipt']['operationType'] == 1:
                print('Продажа')
                date = datetime.datetime.fromtimestamp(
                    int(doc['receipt']['dateTime'])
                ).strftime('%Y-%m-%d')
                doc['receipt']['dateTime'] = doc['receipt']['dateTime'] * 1000
                doc['receipt']['totalSum'] = doc['receipt']['totalSum'] / 100
                doc['receipt']['cashTotalSum'] = doc['receipt']['cashTotalSum'] / 100
                doc['receipt']['ecashTotalSum'] = doc['receipt']['ecashTotalSum'] / 100
                doc['receipt']['prepaidSum'] = doc['receipt']['prepaidSum'] / 100
                doc['receipt']['creditSum'] = doc['receipt']['creditSum'] / 100
                doc['receipt']['provisionSum'] = doc['receipt']['provisionSum'] / 100
                doc['receipt']['nds18'] = doc['receipt']['nds18'] / 100
                for x in doc['receipt']['items']:
                    x['price'] = x['price'] / 100
                    x['sum'] = x['sum'] / 100
                    x['ndsSum'] = x['ndsSum'] / 100

                doctype = 'receipt-'+date
                es = Elasticsearch(ES_URL)
                res = es.index(index=doc['receipt']['userInn'], doc_type=doctype, id=doc['receipt']['fiscalDocumentNumber'], body=doc['receipt'])
        """
        {
            "receipt": {
                "kktRegId": "0000000011038612",
                "fiscalDocumentNumber": 27,
                "dateTime": 1528293720,
                "operationType": 1,
                "totalSum": 1290,
                "items": [
                    {
                        "name": "Чипсы с беконом LAYS",
                        "price": 550,
                        "quantity": 2.345,
                        "sum": 1290,
                        "nds": 1,
                        "ndsSum": 197,
                        "productType": 1,
                        "paymentType": 4
                    }
                ],
                "cashTotalSum": 1290,
                "ecashTotalSum": 0,
                "operator": "СИС. АДМИНИСТРАТОР",
            }
        }
        """

        # Отправляем данные в ОФД
        s.sendall(session_raw + container_raw)

        # Получаем от ОФД ответ
        session_raw_ofd = s.recv(SessionHeader.STRUCT.size)
        session_ofd = SessionHeader.unpack_from(session_raw_ofd)

        container_raw_ofd = s.recv(session_ofd.length)
        # header_raw_ofd, message_raw_ofd = container_raw_ofd[:FrameHeader.STRUCT.size], container_raw_ofd[FrameHeader.STRUCT.size:]
        s.close()

        # Ответ ОФД
        # doc_ofd = unpack_container_message(message_raw_ofd, b'0')[0]
        # print(json.dumps(doc_ofd, ensure_ascii=False, indent=4))

        # Пересылаем ответ ОФД на ККТ
        wr.write(session_raw_ofd + container_raw_ofd)
    finally:
        wr.write_eof()
        wr.drain()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=None, help='хост для запуска сервера')
    parser.add_argument('--port', default=12345, type=int, help='порт для запуска сервера')
    argv = parser.parse_args()
    host = None if argv.host in ['::', 'localhost'] else argv.host

    loop = asyncio.get_event_loop()
    server = asyncio.start_server(handle_connection, host=host, port=argv.port, loop=loop)
    loop.run_until_complete(server)
    print('mock ofd server has been started at port', argv.port)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('received SIGINT, shutting down')

    server.close()

    loop.run_until_complete(server.wait_closed())
    loop.close()
