from flask import Flask, jsonify, request
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
SERVERS = os.environ.get('SERVERS')
cluster = Cluster(SERVERS, auth_provider=auth_provider)

app = Flask(__name__)

session = cluster.connect('meetups', wait_for_all_pools=True)
session.set_keyspace('meetups')
cluster.connect()


def to_json(res):
    result = [{name: (getattr(row, name)) for name in row._fields} for row in res]
    return jsonify(result)


@app.route('/countries', methods=["GET"])
def get_countries():
    res = session.execute("select country from countries group by country;")
    return to_json(res)


@app.route('/cities', methods=["GET"])
def get_cities():
    country = request.args.get('country')
    res = session.execute("select city from countries where country = \'{}\'".format(country))
    return to_json(res)


@app.route('/event', methods=["GET"])
def get_event():
    event_id = request.args.get('event_id')
    res = session.execute(
        "select event_name, event_time, topics, group_name, city, country from events where event_id = \'{}\'"
        .format(event_id)
    )
    return to_json(res)


@app.route('/groups', methods=["GET"])
def get_groups():
    city = request.args.get('city')
    res = session.execute(
        "select group_name, group_id from city_groups where city_name = \'{}\'"
        .format(city)
    )
    return to_json(res)


@app.route('/events', methods=["GET"])
def get_events():
    group_id = request.args.get('group_id')
    res = session.execute(
        "select event_name, event_time, topics, group_name, city, country from groups where group_id= {}"
        .format(group_id))
    return to_json(res)


if __name__ == '__main__':
    app.run()
    cluster.shutdown()
