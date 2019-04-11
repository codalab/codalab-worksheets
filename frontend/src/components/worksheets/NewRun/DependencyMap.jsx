import React, { Component } from 'react';
import CloseIcon from '@material-ui/icons/Close';

export default class DependencyMap extends Component {

  render() {
    const { onChange, onRemove, name, bundle } = this.props
    // Bundle is immutable, but you can change
    // the name it maps to.
    return <tr>
      <td>{ bundle }</td>
      <td><div className="asSeparater">as</div></td>
      <td>
        <input
          className="nameInput"
          defaultValue={ name }
          onChange={ onChange }
        />
      </td>
      <td>
        <CloseIcon onClick={ onRemove } className="closeIcon" />
      </td>
    </tr>;
  }
}
