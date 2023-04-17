import React, { useEffect, useState } from "react"
import "./home.css"
import "react-responsive-carousel/lib/styles/carousel.min.css";
import { Carousel } from 'react-responsive-carousel';
import { Link } from "react-router-dom";
import MovieList from "../../components/movieList/movieList";
import axios from "axios";

const Home = () => {
    const [data, setData] = useState([]);

    const [region, setRegion] = React.useState('India');

    const handleChange = (event) => {
        setRegion(event.target.value);
    };

    const getData = async () => {
        var k = [];
        await axios
            .get(`https://api.themoviedb.org/3/movie/upcoming?api_key=4606882be56455abf5485c21adf285fb&language=en-US&page=1&region=IN`)
            .then((res) => {
                k = res.data.results.filter((i) => i);
                setData(k);
                console.log(data);
            });
    };

    const getGData = async () => {

        var k = [];
        await axios
            .get(`https://api.themoviedb.org/3/movie/upcoming?api_key=4606882be56455abf5485c21adf285fb&language=en-US&page=1`)
            .then((res) => {
                k = res.data.results.filter((i) => i);
                setData(k);
                console.log(k);
            });
    };

    useEffect(() => {
        (region == 'India' ? getData() : getGData())
    }, []);

    const [popularMovies, setPopularMovies] = useState([])

    useEffect(() => {
        // fetch("https://api.themoviedb.org/3/movie/popular?api_key=4606882be56455abf5485c21adf285fb&language=en-US&page=1&region=IN")
        fetch("https://api.themoviedb.org/3/movie/popular?api_key=4606882be56455abf5485c21adf285fb&language=en-US")
            .then(res => res.json())
            .then(data => setPopularMovies(data.results))
    }, [])

    return (
        <>
            <div className="poster">
                <Carousel
                    showThumbs={false}
                    autoPlay={true}
                    transitionTime={3}
                    infiniteLoop={true}
                    showStatus={false}
                >
                    {
                        popularMovies.map((movie, key) => (
                            <Link style={{ textDecoration: "none", color: "white" }} to={`/movie/${movie.id}`} >
                                <div className="posterImage">
                                    <img src={`https://image.tmdb.org/t/p/original${movie && movie.backdrop_path}`} />
                                </div>
                                <div className="posterImage__overlay">
                                    <div className="posterImage__title">{movie ? movie.original_title : ""}</div>
                                    <div className="posterImage__runtime">
                                        {movie ? movie.release_date : ""}
                                        <span className="posterImage__rating">
                                            {movie ? movie.vote_average : ""}
                                            <i className="fas fa-star" />{" "}
                                        </span>
                                    </div>
                                    <div className="posterImage__description">{movie ? movie.overview : ""}</div>
                                </div>
                            </Link>
                        ))
                    }
                </Carousel>
                <MovieList />
            </div>
        </>
    )
}

export default Home