// @flow
import * as React from 'react';
import * as $ from 'jquery';
import TextField from '@material-ui/core/TextField';

function isAscii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}

export default class Editable extends React.Component<{
    label: string,
    value: string,
    method: string,
    url: string,
    canEdit?: boolean,
    classes: {},
    style: {},
    onChange?: () => void,
    buildPayload: (string) => {},
}> {

    /** Prop default values. */
    static defaultProps = {
        method: 'POST',
        canEdit: false,
    };

    constructor(props) {
      super(props);
      this.state = {
        hasError: false,
      };
      this.input = null;
    }

    handleChange = (event) => {
      if (this.input) {
        this.setState({ hasError: !isAscii(this.input.value) });
      }
    }

    handleSubmit(value) {
        return $.ajax({
            type: this.props.method,
            url: this.props.url,
            data: JSON.stringify(this.props.buildPayload(value)),
            contentType: 'application/json; charset=UTF-8',
            dataType: 'json',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
            xhr: function() {
                // Hack for IE < 9 to use PATCH method
                return window.XMLHttpRequest == null ||
                    new window.XMLHttpRequest().addEventListener == null
                    ? new window.ActiveXObject('Microsoft.XMLHTTP')
                    : $.ajaxSettings.xhr();
            },
        }).done(function(response) {
            if (this.props.onChange) this.props.onChange();
        }).fail(function(response, status, err) {
            // TODO: this doesn't stop the value from updating in the frontend
            console.log('Invalid value entered: ', response.responseText);
        });
    }

    handleKeyDown = (event) => {
        const key = event.which || event.keyCode;
        if (key === 13) {
            // Pressed the Enter key.
            this.handleSubmit(this.input.value);
        }
    }

    render() {
        const { classes, style, label, value, canEdit } = this.props;
        const { hasError } = this.state;

        return (
          <TextField
              classes={ classes }
              inputRef={ (ele) => { this.input = ele; } }
              label={ label }
              value={ value || '<none>' }
              margin="dense"
              variant="outlined"
              disabled={ !canEdit }
              style={ style }
              inputProps={ {
                style: { padding: 8 },
              } }
              onChange={ this.handleChange }
              error={ hasError }
              helperText={ hasError ? "Only ASCII characters allowed." : null }
              onKeyDown={ this.handleKeyDown }
            />
        );
    }
}
