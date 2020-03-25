from flask import Flask
 
app = Flask(__name__)
 
@app.route('/')
def index():
    print("hithere")
    return "Hello World!"



# # from utils import generate_random_start, generate_from_seed
# # from keras.models import load_model
# # import tensorflow as tf
# from flask import Flask, render_template, request
# # from wtforms import Form, TextField, validators, SubmitField, DecimalField, IntegerField

# # Create app
# app = Flask(__name__)


# # class ReusableForm(Form):
# #     """User entry form for entering specifics for generation"""
# #     # Starting seed
# #     seed = TextField("Enter a seed string or 'random':", validators=[
# #                      validators.InputRequired()])
# #     # Diversity of predictions
# #     diversity = DecimalField('Enter diversity:', default=0.8,
# #                              validators=[validators.InputRequired(),
# #                                          validators.NumberRange(min=0.5, max=5.0,
# #                                                                 message='Diversity must be between 0.5 and 5.')])
# #     # Number of words
# #     words = IntegerField('Enter number of words to generate:',
# #                          default=50, validators=[validators.InputRequired(),
# #                                                  validators.NumberRange(min=10, max=100, message='Number of words must be between 10 and 100')])
# #     # Submit button
# #     submit = SubmitField("Enter")

# # Home page
# @app.route("/", methods=['GET', 'POST'])
# def home():
#     print("I'm in")
#     print(request)
#     # """Home page of app with form"""
#     # # Create form
#     # form = ReusableForm(request.form)

#     # # On form entry and all conditions met
#     # if request.method == 'POST' and form.validate():
#     #     # Extract information
#     #     seed = request.form['seed']
#     #     diversity = float(request.form['diversity'])
#     #     words = int(request.form['words'])
#     #     # Generate a random sequence
#     #     if seed == 'random':
#     #         return render_template('random.html', input=generate_random_start(model=model, graph=graph, new_words=words, diversity=diversity))
#     #     # Generate starting from a seed sequence
#     #     else:
#     #         return render_template('seeded.html', input=generate_from_seed(model=model, graph=graph, seed=seed, new_words=words, diversity=diversity))
#     # # Send template information to index.html
#     # return render_template('index.html', form=form)


if __name__ == "__main__":
    print(("The website is up"))
    # Run app
    app.run(host="0.0.0.0", port=80)
