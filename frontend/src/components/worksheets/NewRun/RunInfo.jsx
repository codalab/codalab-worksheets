import React, { Component } from 'react';
import TextField from '@material-ui/core/TextField';
import AddIcon from '@material-ui/icons/Add';
import IconButton from '@material-ui/core/IconButton';
import Chip from '@material-ui/core/Chip';

export default class RunInfo extends Component {
  state={
    tag: '',
  }

  tagChange = (event) => {
    this.setState({ tag: event.target.value });
  }

  render() {
    const {
      handleChange,
      addTag,
      removeTag,
      tags,
    } = this.props;

    return <div
      style={ {
        flex: 1,
      } }
    >
      <div className="sectionTitle">Information</div>
      <TextField
        label="Name"
        margin="dense"
        onChange={ handleChange('name') }
      />
      <TextField
        label="Description"
        margin="normal"
        onChange={ handleChange('description') }
        variant="filled"
        rows={ 4 }
        multiline
      />
      <div
        className="row"
        style={ {
          alignItems: 'flex-end',
        } }
      >
        <TextField
          label="Tags"
          margin="normal"
          onChange={ this.tagChange }
        />
        <IconButton
          aria-label="add tag"
          onClick={ () => addTag(this.state.tag) }
        >
          <AddIcon style={ { color: '#225EA8' } } />
        </IconButton>
      </div>
      {
        tags.map((tag, idx) => <Chip
          label={ tag }
          onDelete={ () => removeTag(idx) }
        />)
      }
    </div>;
  }
}
