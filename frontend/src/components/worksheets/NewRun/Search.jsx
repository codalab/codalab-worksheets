import React, { Component } from 'react'
import PropTypes from 'prop-types';
import SearchIcon from '@material-ui/icons/Search';
import IconButton from '@material-ui/core/IconButton';
import Paper from '@material-ui/core/Paper';

import Suggestion from './Suggestion';

const fakeQueryResults = [
  { bundleName: 'abasdsa(0x1a2345)' },
  { bundleName: 'asdcda(0x123b46)' },
  { bundleName: 'basdqwwa(0x12c347)' },
  { bundleName: 'zcxqpon(0xe12348)' },
  { bundleName: 'lmkqmet(0x1f2349)' },
  { bundleName: 'rwcds(0x1235a0)' },
];

export function getSuggestions(value) {
  // Get rid of leading and trailing spaces.
  const inputValue = value.trim().toLowerCase();
  if (inputValue === "") {
    return [];
  }
  // TODO: api call to server to retrieve search results.
  const matches = fakeQueryResults.filter(ele => ele.bundleName.toLowerCase().indexOf(inputValue) >= 0);

  matches.sort((a, b) => a[0] - b[0]);
  // Return all matches
  return matches;
}

class Search extends Component {
  state = {
    query: '',
    suggestions: [],
    highlightIdx: -1,
  }

  suggestion = null;

  handleInputChange = () => {
    // New search query, reset suggestion
    this.suggestion = null;

    const suggestions = getSuggestions(this.search.value);

    this.setState({
      query: this.search.value,
      suggestions,
    });
  }

  handleSearch = () => {
    const { searchHandler } = this.props;
    searchHandler && searchHandler(this.search.value, this.suggestion);
    this.search.value = '';
  }

  handleKeyDown = (event) => {
    const { field } = this.props;
    const { suggestions, highlightIdx } = this.state;

    const key = event.which || event.keyCode;
    
    if (key === 38) {
      // Pressed arrow up key.
      this.setState({
        highlightIdx: Math.max(highlightIdx - 1, -1),
      });
    } else if (key === 40) {
      // Pressed the arrow down key.
      this.setState({
        highlightIdx: Math.min(highlightIdx + 1, suggestions.length - 1),
      });
    } else if (key === 13) {
      // Pressed the Enter key.
      // Clear the suggestions, put the value in the search-bar
      // Do not trigger another suggestions finding mechanism.
      if (suggestions.length > 0) {
        if (highlightIdx >= 0) {
          this.suggestion = suggestions[highlightIdx];
          this.search.value = suggestions[highlightIdx][field];    
        } else{
          // Did not click our suggestion.
          this.suggestion = null;
        }

        this.setState({
          query: '',
          suggestions: [],
          highlightIdx: -1,
        }); 
      } else {
        // There is no suggestion.
        this.suggestion = null;
      }

      this.handleSearch();
    }
  }

  handleSelectSuggestion = (suggestion) => {
    this.suggestion = suggestion;

    const { field } = this.props;

    this.search.value = suggestion[field];
    this.setState({
      query: '',
      suggestions: [],
      highlightIdx: -1,
    });

    this.handleSearch();
  }

  render() {
    const { field } = this.props;
    const { suggestions, query, highlightIdx } = this.state;

    return (<div className="row">
      <div
        style={ { height: 48 } }
        onKeyDown={ this.handleKeyDown }
        tabIndex="0"
      >
        <div className="search-container">
          <input
            type="text"
            className="search-txt"
            placeholder="bundle ID or name"
            ref={ input => { this.search = input; } }
            onChange={ this.handleInputChange }
          />
          <IconButton
            aria-label="Search"
            onClick={ this.handleSearch }
            className="search-btn"
          >
            <SearchIcon style={ { color: 'white' } }/>
          </IconButton>
        </div>
        { suggestions.length > 0 &&
          <Paper>
            {
              suggestions.map((suggestion, idx) =>
                <Suggestion
                  key={ idx }
                  query={ query }
                  suggestion={ suggestion }
                  field={ field }
                  isHighlighted={ idx === highlightIdx }
                  onClick={ this.handleSelectSuggestion }
                />
              )
            }
          </Paper>
        }
      </div>
    </div>);
  }
}

Search.propTypes = {
  searchHandler: PropTypes.func,
  field: PropTypes.string,
};

Search.defaultProps = {
  field: 'bundleName',
}

export default Search;
