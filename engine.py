import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import math

# Another thing we want to do, is give recommendations of movies with a
# certain minimum number of ratings.
# For that, we need to count the number of ratings per movie.
def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable)
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

# TODO:
# filtering: recommendations by genre

class RecommendationEngine:
    """A movie recommendation engine
    """

    # gets called when set in memory has to be recalulated
    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        # http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=als#pyspark.ml.recommendation.ALS
        # class pyspark.ml.recommendation.ALS
        # https://github.com/apache/spark/blob/master/python/pyspark/ml/recommendation.py
        # Ideally, we want to try a large number of combinations of them in order to find
        # the best one.
        # https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html#training-using-als
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter, blocks = self.num_blocks)


        logger.info("ALS model built!")


    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))

        # ########   PUT BACK IN WHEN DEVELOPING!               ########
        # ########   R-M-S-E Root Mean Square Error             ########
        # ########   will tell you how accurate the guess is    ########

        # calculate RMSE with ratings from file and predictions (takes at least a second of request time)
        # predictions = predicted_RDD.map(lambda r: ((r[0], r[1]), r[2]))
        # rates_and_preds = self.ratings_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        # error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        # print '#########  For this data the RMSE is %s' % (error)

        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)

        # example to use the debugger:
        # import pdb; pdb.set_trace()

        # line of this comand takes 0.6 seconds and 1300kb
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def add_ratings(self, ratings):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()

        return ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()

        return ratings

    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])

        return ratings

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()

        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        # genres are NOT taken into account
        # keep for later filtering
        # self.movies_genres_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[2])).cache()

        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()

        # Train the model



        # https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/RecommendationExample.scala

        # dunno yet..
        # implicitPrefs specifies whether to use the explicit feedback ALS variant or one adapted for implicit feedback data.

        # alpha is a parameter applicable to the implicit feedback variant of ALS that governs the baseline confidence in preference observations.
        # alpha is also known as the learning rate: 'step size downhill to minimum'
        # TODO: find out where to stick this
        # small slow, too large -> fail to converge and diverge instead
        self.alpha = 1.0
        # numBlocks is the number of blocks used to parallelize computation (set to -1 to auto-configure).
        self.num_blocks = -1
        # rank is the number of latent factors in the model.
        # http://de.slideshare.net/sscdotopen/latent-factor-models-for-collaborative-filtering
        # conclusion: figure out RMSE like here:
        # https://www.codementor.io/spark/tutorial/building-a-recommender-with-apache-spark-python-example-app-part1
        self.rank = 8
        # what?
        self.seed = 5L
        # iterations is the number of iterations to run.
        self.iterations = 10
        # lambda specifies the regularization parameter in ALS.
        self.regularization_parameter = 0.1
        self.__train_model()

        # save model on disk
        # self.model.save(self.sc, "/tmp/myCollaborativeFilter")

        # load model from disk
        # sameModel = MatrixFactorizationModel.load(sc, "/tmp/myCollaborativeFilter")
