package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MongodbDatabase extends AbstractDatabase {
    
    MongoClient mongoClient;

	public MongodbDatabase() {
		try {
			mongoClient = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

    @Override
    public List<Movie> getAllMovies() {
        List<Movie> movies = new LinkedList<Movie>();
        long startTime = System.currentTimeMillis();
	DB db = mongoClient.getDB("NoSqlProject");
	DBCollection collMovies = db.getCollection("movies");
        
        Iterator<DBObject> cursorMovies = collMovies.find().iterator();
            while (cursorMovies.hasNext()) {
		final DBObject currentMovie = cursorMovies.next();
		movies.add(createMovie(db,currentMovie));
            }
        long endTime = System.currentTimeMillis();
        System.out.println("getAllMovies took " + (endTime - startTime) + " milliseconds (MongoDB)");
	return movies;
    }
    
    private Movie createMovie(DB db,DBObject currentMovie) {
		try {

			DBCollection collectMovieGenres = db.getCollection("mov_genre");
			DBCollection collectGenres = db.getCollection("genres");
                        
			List<Genre> currentMovieGenres = new ArrayList<Genre>();
			BasicDBObject movieGenreFilter = new BasicDBObject();
                        
                        int currentMovieId = Integer.parseInt(currentMovie.get("id").toString());
                        
			movieGenreFilter.put("mov_id", currentMovieId);

			Iterator<DBObject> cursorMovieGenre = collectMovieGenres.find(movieGenreFilter).iterator();
			while (cursorMovieGenre.hasNext()) {
				final DBObject currentMovieGenreAssociation = cursorMovieGenre.next();

				BasicDBObject genreFilter = new BasicDBObject();
                                int currentMovieGenreAssociationGenre = Integer.parseInt(currentMovieGenreAssociation.get("genre").toString());
				genreFilter.put("id", currentMovieGenreAssociationGenre);

				DBObject currentGenre = collectGenres.findOne(genreFilter);
                                int currentGenreId = Integer.parseInt(currentGenre.get("id").toString());
				currentMovieGenres.add(new Genre(currentGenreId, currentGenre.get("name").toString()));
			}
			return new Movie(Integer.parseInt(currentMovie.get("id").toString()),currentMovie.get("title").toString(), currentMovieGenres);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
                

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        List<Movie> movies = new LinkedList<Movie>();
	DB db = mongoClient.getDB("NoSqlProject");
	DBCollection collectRatings = db.getCollection("ratings");
	DBCollection collectMovies = db.getCollection("movies");

	BasicDBObject userRatingsFilter = new BasicDBObject();
	userRatingsFilter.put("user_id", userId);
	Iterator<DBObject> cursorRatings = collectRatings.find(userRatingsFilter).iterator();
        
	while (cursorRatings.hasNext()) {
		final DBObject currentRating = cursorRatings.next();
		BasicDBObject ratingMoviesFilter = new BasicDBObject();
		ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
		DBObject currentMovie = collectMovies.findOne(ratingMoviesFilter);
		movies.add(createMovie(db,currentMovie));
	}

	return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        List<Rating> ratings = new LinkedList<Rating>();
        DB db = mongoClient.getDB("MoviesProj");
        DBCollection collectRatings = db.getCollection("ratings");
	DBCollection collectMovies = db.getCollection("movies");

	BasicDBObject userRatingsFilter = new BasicDBObject();
	userRatingsFilter.put("user_id", userId);
	Iterator<DBObject> cursorRatings = collectRatings.find(userRatingsFilter).iterator();
        
	while (cursorRatings.hasNext()) {
		final DBObject currentRating = cursorRatings.next();
		BasicDBObject ratingMoviesFilter = new BasicDBObject();
		ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
		DBObject currentMovie = collectMovies.findOne(ratingMoviesFilter);
		ratings.add(new Rating(createMovie(db,currentMovie), userId, Integer.parseInt(currentRating.get("rating").toString())));
	}
        
	return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
	DB db = mongoClient.getDB("MoviesProj");
	DBCollection collectionRatings = db.getCollection("ratings");

	BasicDBObject ratingFilter = new BasicDBObject();
	ratingFilter.put("user_id", rating.getUserId());
	ratingFilter.put("mov_id", rating.getMovieId());
	DBObject cursorRatings = collectionRatings.findOne(ratingFilter);
        
	if(cursorRatings != null) {
		BasicDBObject updateFields = new BasicDBObject();
                BasicDBObject setQuery = new BasicDBObject();
                
		updateFields.append("rating", rating.getScore());
		setQuery.append("$set", updateFields);

		collectionRatings.update(ratingFilter, setQuery);
	}
	else {
		BasicDBObject createFields = new BasicDBObject();
                
		createFields.append("user_id", rating.getUserId());
		createFields.append("mov_id", rating.getMovieId());
		createFields.append("rating", rating.getScore());
		collectionRatings.insert(createFields);
	}
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }    
}
