import * as React from 'react';

class Button extends React.Component {
    render() {
        var buttonStyle = this.props.flexibleSize
            ? {
                  padding: '5px',
              }
            : {
                  height: this.props.height ? this.props.height + 'px' : '30px',
                  width: this.props.width ? this.props.width + 'px' : '60px',
              };

        var classname = 'button button-' + this.props.type;
        if (this.props.className) {
            classname = classname + ' ' + this.props.className;
        }
        return (
            <button
                style={buttonStyle}
                className={classname}
                id={this.props.id}
                ref={this.props.innerRef}
                onClick={this.props.handleClick}
            >
                {this.props.text}
            </button>
        );
    }
}

export default Button;
