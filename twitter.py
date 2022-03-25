from flask import Flask, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, login_required, logout_user, \
    current_user, login_user
from werkzeug.exceptions import NotFound, BadRequest
from database import Database
from datetime import datetime


db_name = "twitter"
db_schema_file = "schema.txt"
flask_secret_key = "SUPERSUPERSECRET"


class CURD(object):
    def __init__(self, db_name, schema_file):
        self.db = Database(db_name, schema_file)

    def add_user(self, username, password):
        now = datetime.utcnow()
        q = f"INSERT INTO users VALUES ('{username}', '{password}', '{now}');"
        try:
            return self.db.run_query(q)[0]
        except IndexError:
            return

    def get_user(self, username, password):
        q = f"SELECT FROM users WHERE username == '{username}' and password == '{password}';"
        try:
            return self.db.run_query(q)[0]
        except (IndexError, ValueError):
            return

    def get_user_by_id(self, user_id):
        q = f"SELECT FROM users WHERE id == '{user_id}';"
        try:
            return self.db.run_query(q)[0]
        except IndexError:
            return

    def add_tweet(self, user_id, text: str = None, retweet_id: int = None):
        try:
            user = self.get_user_by_id(user_id)
            assert user
        except (ValueError, AssertionError):
            raise ValueError('User not found')

        now = datetime.utcnow()
        if text:
            retweet_id = 0
            retweet_username = ''
        elif retweet_id:
            retweet = self.get_tweet(retweet_id)
            retweet_id = retweet['id']
            retweet_username = retweet['user_username']
            if user['username'] == retweet_username:
                raise ValueError('You cannot retweet your own tweet')
            text = retweet['text']
        else:
            raise ValueError('You should define at least one of (text, retweet_id)')

        text = text.replace("'", "\\'")
        q = f"INSERT INTO tweets VALUES ('{user['id']}', '{user['username']}', '{text}', '{now}', '{retweet_id}', '{retweet_username}', 0);"
        try:
            return self.db.run_query(q)[0]
        except IndexError:
            return

    def get_tweet(self, tweet_id):
        q = f"SELECT FROM tweets WHERE id == '{tweet_id}';"
        try:
            t = self.db.run_query(q)[0]
            t['text'] = t['text'].replace('\\n', '\n').replace("\\'", "'")
            return t
        except IndexError:
            return

    def get_tweets(self, limit=20):
        q = "SELECT FROM tweets;"
        tweets = self.db.run_query(q, select_limit=limit, select_reverse=True)
        for t in tweets:
            t['text'] = t['text'].replace('\\n', '\n').replace("\\'", "'")
        return tweets

    def is_liker(self, user_id, tweet_id):
        q = f"SELECT FROM tweet_likes WHERE tweet_id == {tweet_id} AND user_id == {user_id};"
        liked = self.db.run_query(q, select_limit=1)
        return bool(liked)

    def get_user_likes(self, user_id, limit=20):
        q = f"SELECT FROM tweet_likes WHERE user_id == {user_id};"
        likes = self.db.run_query(q, select_limit=limit, select_reverse=True)
        return [like['tweet_id'] for like in likes]

    def switch_like_tweet(self, user_id, tweet_id):
        q = f"SELECT FROM tweets WHERE id == {tweet_id};"
        t = self.db.run_query(q, select_limit=1)
        if not t:
            raise ValueError("tweet not found")
        t = t[0]

        if self.is_liker(user_id, tweet_id):
            q = f"DELETE FROM tweet_likes WHERE user_id == {user_id} AND tweet_id == {tweet_id};"
            q += f"UPDATE tweets WHERE id == {tweet_id} VALUES "
            q += f" ('{t['user_id']}', '{t['user_username']}', '{t['text']}',"
            q += f"'{t['posted_at']}', '{t['retweet_id']}', '{t['retweet_from_username']}', '{t['likes']-1}');"
            self.db.run_query(q)
        else:
            q = f"INSERT INTO tweet_likes VALUES ({tweet_id}, {user_id});"
            q += f"UPDATE tweets WHERE id == {tweet_id} VALUES "
            q += f" ('{t['user_id']}', '{t['user_username']}', '{t['text']}',"
            q += f"'{t['posted_at']}', '{t['retweet_id']}', '{t['retweet_from_username']}', '{t['likes']+1}');"
            self.db.run_query(q)

    def get_tweet_likes_count(self, tweet_id):
        q = f"SELECT FROM tweet_likes WHERE tweet_id == {tweet_id};"
        return len(self.db.run_query(q))

    def get_tweet_likers(self, tweet_id):
        q = f"SELECT FROM tweet_likes WHERE tweet_id == {tweet_id};"
        likes = self.db.run_query(q)
        ors = set()
        for like in likes:
            ors.add(f"id == {like['user_id']}")

        where = ' OR '.join(ors)
        if not where:
            return []

        q = f"SELECT FROM users WHERE {where};"
        return self.db.run_query(q)

    def delete_tweet(self, tweet_id):
        q = f"DELETE FROM tweets WHERE id == {tweet_id} AND user_id == {current_user.id};"
        return self.db.run_query(q)


app = Flask(__name__)
app.config['SECRET_KEY'] = flask_secret_key
curd = CURD(db_name, db_schema_file)
login_manager = LoginManager()
login_manager.init_app(app)


class User(object):
    def __init__(self, user_id):
        '''user_id = username:id'''
        user, _, id = user_id.rpartition(':')
        self.user_id = user_id
        self.username = user
        self.id = int(id)

    def is_active(self):
        return True

    def get_id(self):
        return self.user_id

    def is_authenticated(self):
        return True

    def is_anonymous(self):
        return False


@login_manager.user_loader
def load_user(user_id):
    return User(user_id)


@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('tweets'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if not username or not password:
            flash('username or password is wrong.', 'danger')
            return render_template('login.html')

        user = curd.get_user(username, password)
        if user:
            user_id = f"{user['username']}:{user['id']}"
            login_user(User(user_id))
            return redirect(url_for('tweets'))
        else:
            flash('username or password is wrong.', 'danger')

    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('tweets'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if not username or not password:
            flash('username or password is wrong.', 'danger')
            return render_template('register.html')

        try:
            user = curd.add_user(username, password)
            if user:
                flash('User registered successfully, login now!', 'success')
                return redirect(url_for('login'))
            else:
                flash('Something went wrong!', 'danger')
        except ValueError as err:
            if 'duplicate' in str(err):
                flash('This username already exists', 'danger')
            else:
                flash('Something went wrong!', 'danger')

    return render_template('register.html')


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route("/")
@login_required
def tweets():
    tweets = curd.get_tweets()
    my_likes = curd.get_user_likes(current_user.id)
    my_tweets = [t['id'] for t in filter(
        lambda t: t['user_id'] == current_user.id, tweets)]
    return render_template('tweets.html',
                           tweets=tweets,
                           me=current_user,
                           my_likes=my_likes,
                           my_tweets=my_tweets)


@app.route("/like/<int:tweet_id>")
@login_required
def like(tweet_id):
    curd.switch_like_tweet(current_user.id, tweet_id)
    return redirect(url_for('tweets') + f'#t{tweet_id}')


@app.route("/retweet/<int:tweet_id>")
@login_required
def retweet(tweet_id):
    tweet = curd.get_tweet(tweet_id)
    if not tweet:
        raise NotFound()
    return render_template('retweet.html', tweet=tweet)


@app.route("/retweet_confirm/<int:tweet_id>")
@login_required
def retweet_confirm(tweet_id):
    try:
        tweet_id = curd.add_tweet(current_user.id, retweet_id=tweet_id)
    except (TypeError, ValueError) as err:
        if str(err) == 'You cannot retweet your own tweet':
            raise BadRequest('You cannot retweet your own tweet')
        raise NotFound()
    return redirect(url_for('tweets') + f'#t{tweet_id}')


@app.route("/tweet", methods=['POST'])
@login_required
def tweet():
    text = request.form.get('text')
    if not text:
        raise BadRequest()
    try:
        tweet_id = curd.add_tweet(current_user.id, text)
    except ValueError as err:
        print(err)
        raise BadRequest()
    return redirect(url_for('tweets') + f'#t{tweet_id}')


@app.route("/delete_tweet/<int:tweet_id>")
@login_required
def delete_tweet(tweet_id):
    curd.delete_tweet(tweet_id)
    return redirect(url_for('tweets'))


@app.route("/likes/<int:tweet_id>")
@login_required
def likes(tweet_id):
    likers = curd.get_tweet_likers(tweet_id)
    return render_template('likes.html', likers=likers)


if __name__ == '__main__':
    app.run(debug=True)
