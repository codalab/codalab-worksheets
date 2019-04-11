import React, { Component } from 'react';
import Drawer from '@material-ui/core/Drawer';
import Fab from '@material-ui/core/Fab';
import UploadIcon from '@material-ui/icons/CloudUpload';
import Input from '@material-ui/core/Input';
import { MuiThemeProvider } from '@material-ui/core/styles';

import theme from './theme.js';

// React doesn't support transpilation of directory and
// webkitdirectory properties, have to set them
// programmatically.
class InputDir extends Component {
  
  componentDidMount() {
    this.inputDir.directory = true;
    this.inputDir.webkitdirectory = true;
  }

  render() {
    const { eleref, ...others } = this.props;
    return <input
      {...others}
      type="file"
      ref={ (ele) => { this.inputDir = ele; eleref(ele); } }
    />;
  }
}

export default class UploadBundle extends Component {
  
  state = {
    bottom: false,
  }

  toggleDrawer = (bool) => () => {
    this.setState({ bottom: bool });
  }

  dropFile = (e) => {
    let dt = e.dataTransfer;
    let files = dt.files;

    // TODO: ...
    e.target.style.opacity = 1.0;
    e.preventDefault();
    e.stopPropagation();
  }

  dropDir = (e) => {
    let dt = e.dataTransfer;
    let files = dt.files;

    // TODO: actually post files to BE
    e.target.style.opacity = 1.0;
    e.preventDefault();
    e.stopPropagation();
  }

  highlight = (e) => {
    e.target.style.opacity = .5;
    e.preventDefault();
    e.stopPropagation();
  }

  unhighlight = (e) => {
    e.target.style.opacity = 1.0;
    e.preventDefault();
    e.stopPropagation();
  }

  render() {
    return <div>
      <MuiThemeProvider theme={ theme }>
        <Fab
          variant="extended"
          size="medium"
          color="primary"
          aria-label="Add"
          onClick={ this.toggleDrawer(true) }
        >
          <UploadIcon style={ { marginRight: 16 } } />
          Upload
        </Fab>
      </MuiThemeProvider>
      <Drawer
        anchor="bottom"
        open={ this.state.bottom }
        onClose={ this.toggleDrawer(false) }
        PaperProps={ { style: {
          minHeight: '25vh',
          width: '80vw',
          marginLeft: '10vw',
          borderTopLeftRadius: 8,
          borderTopRightRadius: 8,
        } } }
      >
        <div
          style={ {
            display: 'flex',
            flexDirection: 'row',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: 16,
          } }
        >
          <div
            style={ {
              display: 'flex',
              flexDirection: 'row',
              width: '60%',
            } }
          >
            <div
              style={ {
                ...styles.inputBoxStyle,
                backgroundColor: 'rgba(85, 128, 168, 0.2)',
                borderColor: 'rgba(85, 128, 168, 0.2)',
              } }
              onClick={ () => { this.inputDir.click(); } }
              onDrop={ this.dropDir }
              onDragOver={ this.highlight }
              onDragLeave={ this.unhighlight }
            >
              <InputDir
                eleref={ (ele) => { this.inputDir = ele; } }
                style={ { visibility: 'hidden', position: 'absolute' } }
              />
              <div style={ styles.greyText }>Drop Folder</div>
            </div>
            <div
              style={ {
                ...styles.inputBoxStyle,
                backgroundColor: 'rgba(255, 175, 125, 0.2)',
                borderColor: 'rgba(255, 175, 125, 0.2)',
              } }
              onClick={ () => { this.inputFile.click(); } }
              onDrop={ this.dropFile }
              onDragOver={ this.highlight }
              onDragLeave={ this.unhighlight }
            >
              <input
                type="file"
                style={ { visibility: 'hidden', position: 'absolute' } }
                ref={ (ele) => { this.inputFile = ele; } }
              />
              <div style={ styles.greyText }>Drop File</div>
            </div>
          </div>
          <div style={ styles.blueText }>Or</div>
          <div>
            <p style={ styles.blueText }>
              Copy bundle from URL:
            </p>
            <Input
              placeholder="url here..."
              inputProps={ {
                'aria-label': 'URL',
              } }
            />
          </div>
        </div>
      </Drawer>
    </div>;
  }
}

const styles = {
  blueText: {
    color: '#225EA8',
  },
  greyText: {
    color: '#666666',
  },
  inputBoxStyle: {
    flex: 1,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: 200,
    borderRadius: 8,
    border: '2px dashed',
    cursor: 'pointer',
  }
};
