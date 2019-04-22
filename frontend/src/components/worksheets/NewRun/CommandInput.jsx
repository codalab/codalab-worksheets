import React, { Component } from 'react';

export default class CommandInput extends Component {
  state = {
    command: '',
  }

  handleChange = (event) => {
    this.setState({ command: event.target.value });
  }

  render() {
    return <div
      className="row commandInputContainer"
    >
      <div style={ { marginLeft: 8 } }>$</div>
      <input
        placeholder="type command here (e.g python train.py -d data/trainset.txt)"
        onChange={ this.handleChange }
        className="commandInput"
      />
    </div>;
  }
}
