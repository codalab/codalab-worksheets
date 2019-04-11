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
        placeholder="Command"
        onChange={ this.handleChange }
        className="commandInput"
      />
    </div>;
  }
}
