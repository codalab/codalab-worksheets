import React from 'react';
import NativeSelect from '@material-ui/core/NativeSelect';
import Input from '@material-ui/core/Input';
import Checkbox from '@material-ui/core/Checkbox';

const Configuration = (props) => {
  const {
    handleChange,
    handleCheck,
    network,
    failedOkay } = props;

  return <div
    style={ { flex: 1, marginRight: 16 } }
  >
    <div className="sectionTitle">Configuration</div>
    <div className="row configSection">
      <div className="inlineLabel">
        Docker Image
      </div>
      <NativeSelect
        defaultValue="codalab/ubuntu:1.9"
        onChange={ handleChange('docker') }
        input={
          <Input
            name="Unit"
            inputProps={ {
              style: { paddingLeft: 8 }
            } }
          />
        }
      >
        <option value="codalab/ubuntu:1.9">codalab/ubuntu:1.9</option>
        <option value="codalab/ubuntu:1.8">codalab/ubuntu:1.8</option>
        <option value="codalab/ubuntu:1.7">codalab/ubuntu:1.7</option>
        <option value="codalab/ubuntu:1.6">codalab/ubuntu:1.6</option>
      </NativeSelect>
    </div>
    <div className="row configSection">
      <div className="inlineLabel">Memory</div>
      <Input
        defaultValue={ 2 }
        style={ {
          width: 60,
          borderRight: '1px solid #ccc',
        } }
        onChange={ handleChange('memVal') }
      />
      <NativeSelect
        defaultValue={"g"}
        onChange={ handleChange('memUnit') }
        input={
          <Input
            name="Unit"
            inputProps={ {
              style: { paddingLeft: 8 }
            } }
          />
        }
      >
        <option value=""/>
        <option value="k">K</option>
        <option value="m">M</option>
        <option value="g">G</option>
        <option value="t">T</option>
      </NativeSelect>
    </div>
    <div className="row configSection">
      <div className="inlineLabel">Disk</div>
      <Input
        defaultValue={ 10 }
        style={ {
          width: 60,
          borderRight: '1px solid #ccc',
        } }
        onChange={ handleChange('diskVal') }
      />
      <NativeSelect
        defaultValue={"g"}
        onChange={ handleChange('diskUnit') }
        input={
          <Input
            name="Unit"
            inputProps={ {
              style: { paddingLeft: 8 }
            } }
          />
        }
      >
        <option value=""/>
        <option value="k">K</option>
        <option value="m">M</option>
        <option value="g">G</option>
        <option value="t">T</option>
      </NativeSelect>
    </div>
    <div className="row configSection">
      <div className="inlineLabel">CPUs</div>
      <Input
        defaultValue={ 2 }
        style={ {
          width: 60,
        } }
        onChange={ handleChange('cpus') }
      />
    </div>
    <div className="row configSection">
      <div className="inlineLabel">GPUs</div>
      <Input
        style={ {
          width: 60,
        } }
        onChange={ handleChange('gpus') }
      />
    </div>
    <div className="row configSection">
      <div className="inlineLabel">Allocate Time</div>
      <Input
        style={ {
          width: 60,
          borderRight: '1px solid #ccc',
        } }
        onChange={ handleChange('time') }
      />
      <NativeSelect
        defaultValue={"g"}
        onChange={ handleChange('time') }
        input={
          <Input
            name="Unit"
            inputProps={ {
              style: { paddingLeft: 8 }
            } }
          />
        }
      >
        <option value=""/>
        <option value="k">m</option>
        <option value="m">h</option>
        <option value="g">d</option>
      </NativeSelect>
    </div>
    <div className="row">
      <Checkbox
        checked={ network }
        onChange={ handleCheck('network') }
        value="checkedA"
      />
      <div className="inlineLabel">
        Allow Network Access
      </div>
    </div>
    <div className="row">
      <Checkbox
        checked={ failedOkay }
        onChange={ handleCheck('failedOkay') }
        value="checkedA"
      />
      <div className="inlineLabel">
        Allow failed Dependencies
      </div>
    </div>
  </div>;
}

export default Configuration;
