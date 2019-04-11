import React, { Component } from 'react';
import { MuiThemeProvider } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Fab from '@material-ui/core/Fab';
import AddIcon from '@material-ui/icons/AddCircle';

import Search from './Search';
import DependencyMap from './DependencyMap';
import Configuration from './Configuration';
import RunInfo from './RunInfo';
import CommandInput from './CommandInput';
import './NewRun.css';
import theme from './theme.js';

export default class NewRun extends Component {

  // dependencies if an array of { name, bundle } can be
  // interpreted as "bundle as name", a key value pair becomes
  // an object like { name: data, bundle: data(0x41a160) }.
  state = {
    bottom: false,
    dependencies: [],
    tags: [],
  }

  toggleDrawer = (bool) => () => {
    this.setState({ bottom: bool });
  }

  addDependency = (query, bundle) => {
    if (!bundle) {
      // TODO: didn't find anything.
      return;
    }
    const { dependencies } = this.state;
    const nDep = {
      name: bundle.bundleName.substring(0, bundle.bundleName.indexOf('(')),
      bundle: bundle.bundleName,
    };
    dependencies.push(nDep);
    this.setState({ dependencies });
  }

  updateDependencyMapping = (idx) => (event) => {
    const newName = event.target.value;
    this.setState((state) => {
      const { dependencies } = this.state;
      const bundle = dependencies[idx];
      bundle.name = newName;
      return { dependencies };
    });
  }

  removeDependency = (idx) => () => {
    const { dependencies } = this.state;
    dependencies.splice(idx, 1);
    this.setState({ dependencies });
  }

  handleChange = name => event => {
    this.setState({ [name]: event.target.value });
  }

  handleCheck = name => event => {
    this.setState({ [name]: event.target.checked });
  }

  addTag = (tag) => {
    if (!tag) {
      return;
    }
    const { tags } = this.state;
    tags.push(tag);
    this.setState({ tags });
  }

  removeTag = (idx) => {
    const { tags } = this.state;
    tags.splice(idx, 1);
    this.setState({ tags });
  }

  render() {
    const {
      bottom,
      dependencies,
      network,
      failedOkay,
      tags,
    } = this.state;

    return <div>
      <MuiThemeProvider theme={ theme }>
      <Fab
        variant="extended"
        size="medium"
        color="primary"
        aria-label="Add"
        onClick={ this.toggleDrawer(true) }
      >
        <AddIcon style={ { marginRight: 16 } } />
        New Run
      </Fab>
      <Drawer
        anchor="bottom"
        open={ bottom }
        onClose={ this.toggleDrawer(false) }
        PaperProps={ { style: {
          minHeight: '75vh',
          width: '90vw',
          marginLeft: '5vw',
          borderTopLeftRadius: 8,
          borderTopRightRadius: 8,
        } } }
      >
        <div
          style={ {
            display: 'flex',
            flexDirection: 'column',
            flex: 1,
            padding: 16,
            justifyContent: 'space-between',
          } }
        >
          <div
            style={ {
              display: 'flex',
              flexDirection: 'row',
              justifyContent: 'space-between',
              flex: 1,
              zIndex: 10,
            } }
          >
            <div
              style={ { flex: 1, marginRight: 16 } }
            >
              <div className="sectionTitle">Dependencies</div>
              <table>
                <tbody>
                {
                  dependencies
                  .map((ele, idx) => <DependencyMap
                    key={ idx }
                    name={ ele.name }
                    bundle={ ele.bundle }
                    onChange={
                      this.updateDependencyMapping(idx)
                    }
                    onRemove={
                      this.removeDependency(idx)
                    }
                  />)
                }
                </tbody>
              </table>
              <Search
                searchHandler={ this.addDependency }
              />
            </div>
            <Configuration
              handleChange={ this.handleChange }
              handleCheck={ this.handleCheck }
              network={ network }
              failedOkay={ failedOkay }
            />
            <RunInfo
              handleChange={ this.handleChange }
              addTag={ this.addTag }
              removeTag={ this.removeTag }
              tags={ tags }
            />
          </div>
          <CommandInput />
        </div>
      </Drawer>
      </MuiThemeProvider>
    </div>;
  }
}