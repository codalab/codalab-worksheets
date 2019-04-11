import React from 'react';
import MenuItem from '@material-ui/core/MenuItem';
import match from 'autosuggest-highlight/match';
import parse from 'autosuggest-highlight/parse';

const Suggestion = (props) => {
  const {
    query,
    suggestion,
    field,
    isHighlighted,
    onClick } = props;

  const matches = match(suggestion[field], query);
  const parts = parse(suggestion[field], matches);

  return <MenuItem
    selected={ isHighlighted }
    component="div"
    onClick={ () => onClick(suggestion)  }
  >
    <div style={ { color: 'black' } }>
      {
        parts.map((part, index) => {
          return part.highlight ? (
            <span key={ `${ index }` } style={ { fontWeight: 500 } }>
              { part.text }
            </span>
          ) : (
            <strong key={ `${ index }` } style={ { fontWeight: 300 } }>
              { part.text }
            </strong>
          );
        })
      }
    </div>
  </MenuItem>;
};

export default Suggestion;
