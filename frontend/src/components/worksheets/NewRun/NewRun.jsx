// @flow
import * as React from 'react';
import $ from 'jquery';

import { withStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';

import AddIcon from '@material-ui/icons/PlayCircleFilled';
import DeleteIcon from '@material-ui/icons/Delete';

import Select, { components } from 'react-select';

import ConfigPanel, {
    ConfigLabel,
    ConfigTextInput,
    ConfigChipInput,
    ConfigCodeInput,
    ConfigSwitchInput,
} from '../ConfigPanel';

import {
    depEqual,
    shorten_uuid,
    buildTerminalCommand,
    createHandleRedirectFn,
} from '../../../util/worksheet_utils';


type Bundle = { name: string, uuid: string, path?: string };
type Dependency = { target: Bundle, alias: string };

// TODO: Remove dummy data!
const kDummyCandidates: Bundle[] = [
    { name: "bundle-1", uuid: "0x111111" },
    { name: "bundle-2", uuid: "0x222222" },
    { name: "bundle-3", uuid: "0x333333" },
    { name: "bundle-4", uuid: "0x444444" },
    { name: "bundle-5", uuid: "0x555555" },
    { name: "bundle-6", uuid: "0x666666" },
    { name: "bundle-7", uuid: "0x777777" },
    { name: "bundle-8", uuid: "0x888888" },
    { name: "bundle-9", uuid: "0x999999" },
];

class DependencyEditorRaw extends React.Component<{
    /** JSS styling object. */
    classes: {},

    /** Functions to update state. */
    addDependency: (Bundle) => void,
    updateDependency: (number, string) => void,
    removeDependency: (number) => void,

    /** Candidate dependencies. */
    dependencies: Dependency[],
    candidates?: Bundle[],
}> {
    static defaultProps = {
        dependencies: [],
        candidates: [],
    };
    render() {
        const { classes, dependencies, candidates,
            addDependency, updateDependency, removeDependency } = this.props;

        return (
            <Grid container direction="column" className={classes.container}>
                {/* Existing dependencies ------------------------------------------------------ */}
                {dependencies.map((dep, idx) => (
                    <Grid item container direction="row" key={idx}>
                        <Grid item xs={4}>
                            <Typography variant="body1">
                                {`${dep.target.name} (${shorten_uuid(dep.target.uuid)})`}
                            </Typography>
                        </Grid>
                        <Grid item xs={1} container justify="center">
                            <Typography variant="body2">as</Typography>
                        </Grid>
                        <Grid item xs={3}>
                            <ConfigTextInput
                                value={dep.alias}
                                onValueChange={(alias) => updateDependency(idx, alias)}
                            />
                        </Grid>
                        <Grid item xs={1} container justify="center">
                            <IconButton
                                onClick={() => removeDependency(idx)}>
                                <DeleteIcon fontSize="small" />
                            </IconButton>
                        </Grid>
                    </Grid>
                ))}

                {/* New dependency ------------------------------------------------------------- */}
                <Grid item container direction="row" key={-1}>
                    <Grid item xs={4}>
                        <Select
                            options={candidates.map((bundle) => ({
                                label: `${bundle.name} (${shorten_uuid(bundle.uuid)})`,
                                value: bundle,
                            }))}
                            value=""
                            onChange={(option) => addDependency(option.value)}
                            placeholder="target"
                            noOptionsMessage={() => "No matching bundles"}
                            components={{
                                IndicatorsContainer: (props) => null,
                            }}
                            styles={{
                                control: (provided) => ({
                                    ...provided,
                                    minHeight: 30,
                                }),
                            }}
                        />
                    </Grid>
                    <Grid item xs={1} container justify="center">
                        <Typography variant="body2">as</Typography>
                    </Grid>
                    <Grid item xs={3}>
                        <ConfigTextInput disabled value="alias"/>
                    </Grid>
                </Grid>
            </Grid>
        );
    }
}
const DependencyEditor = withStyles((theme) => ({
    container: {
        paddingBottom: theme.spacing.large,
    }
}))(DependencyEditorRaw);


class NewRun extends React.Component<{
    /** JSS styling object. */
    classes: {},

    /** Worksheet info. */
    ws: {},

    onSubmit: () => void,
}, {
    dependencies: Dependency[],
    command: string,
    name: string,
    description: string,
    tags: string[],
    disk: string,
    memory: string,
    cpu: number,
    gpu: number,
    docker: string,
    networkAccess: boolean,
    failedDependencies: boolean,
}> {

    defaultConfig = {
        dependencies: [],
        command: "",
        name: 'untitled-run',
        description: '',
        tags: [],
        disk: "10g",
        memory: "2g",
        cpu: 1,
        gpu: 1,
        docker: "codalab/default-cpu:latest",
        networkAccess: false,
        failedDependencies: false,
    }

    /**
     * Constructor.
     * @param props
     */
    constructor(props) {
        super(props);
        this.state = {
            ...this.defaultConfig,
        };
    }

    /**
     * Add a new dependency to the list, with the alias defaulting to the bundle name.
     * @param bundle
     *     Bundle object of target.
     */
    addDependency(bundle: Bundle) {
        if (!bundle) return;
        const { dependencies } = this.state;
        this.setState({ dependencies: [...dependencies, { target: bundle, alias: bundle.name }] });
    }

    /**
     * Change the alias of a particular dependency.
     * @param idx
     *     List index of dependency to be changed.
     * @param alias
     *     New alias of the dependency.
     */
    updateDependency(idx: number, alias: string) {
        const { dependencies } = this.state;
        dependencies[idx].alias = alias;
        this.setState({ dependencies });
    }

    /**
     * @param idx
     */
    removeDependency(idx: number) {
        const { dependencies } = this.state;
        dependencies.splice(idx, 1);
        this.setState({ dependencies });
    }

    getCommand() {
        const {
            dependencies,
            command,
            name,
            description,
            tags,
            disk,
            memory,
            cpu,
            gpu,
            docker,
            networkAccess,
            failedDependencies,
        } = this.state;
        const { after_sort_key } = this.props;

        let args = ['run'];

        // if (after_sort_key) args.push(`-a ${ after_sort_key }`);
        if(name) args.push(`--name ${name}`);
        if(description) args.push(`--description ${description}`);
        //if(tags) args.push(`--tags ${tags.map((tag) => `'${tag}'`).join(",")}`);
        if(disk) args.push(`--request-disk ${disk}`);
        if(memory) args.push(`--request-memory ${memory}`);
        if(cpu) args.push(`--request-cpus ${cpu}`);
        if(gpu) args.push(`--request-gpus ${gpu}`);
        if(docker) args.push(`--request-docker-image ${docker}`);
        if(networkAccess) args.push(`--request-network`);
        if(failedDependencies) args.push(`--allow-failed-dependencies`);

        for (let dep of dependencies) {
            const key = dep.alias;
            let value = shorten_uuid(dep.target.uuid);
            if(dep.target.path) value += '/' + dep.target.path;
            args.push(key + ':' + value);
        }

        if(command) args.push(`"${command}"`);

        return args.join(" ");  // TODO: Make safe.
        //return buildTerminalCommand(args);
    }

    runCommand() {
        const cmd = this.getCommand();
        const response = $('#command_line').terminal().exec(cmd);
    }

    /**
     * Render.
     */
    render() {
        const { classes, ws } = this.props;

        let candidates: Bundle[] = [];
        if(ws && ws.info && ws.info.items) {
            ws.info.items.forEach((item) => {
                if(item.bundles_spec && item.bundles_spec.bundle_infos) {
                    item.bundles_spec.bundle_infos.forEach((bundle) => {
                        candidates.push({
                            name: bundle.metadata.name,
                            uuid: bundle.uuid,
                            path: null,
                        });
                    });
                }
            });
        }

        return (
            <ConfigPanel
                buttons={(
                    <div>
                        <Button
                            variant='text'
                            color='primary'
                            onClick={() => this.setState(this.defaultConfig)}
                        >Clear</Button>
                        <Button
                            variant='contained'
                            color='primary'
                            onClick={() => {
                                this.runCommand();
                                this.props.onSubmit();
                            }}
                        >Confirm</Button>
                    </div>
                )}
                sidebar={(
                    <div>
                        <Typography variant='subtitle1'>Information</Typography>

                        <ConfigLabel
                            label="Name"
                            tooltip="Short name (not necessarily unique) to provide an
                            easy, human-readable way to reference this bundle (e.g as a
                            dependency). May only use alphanumeric characters and dashes."
                        />
                        <ConfigTextInput
                            value={this.state.name}
                            onValueChange={(value) => this.setState({ name: value })}
                            placeholder="untitled-run"
                        />

                        <ConfigLabel
                            label="Description"
                            tooltip="Text description or notes about this bundle."
                            optional
                        />
                        <ConfigTextInput
                            value={this.state.description}
                            onValueChange={(value) => this.setState({ description: value })}
                            multiline
                            maxRows={3}
                        />

                        <ConfigLabel
                            label="Tags"
                            tooltip="Keywords that can be used to search for and categorize
                            this bundle."
                            optional
                        />
                        <ConfigChipInput
                            values={this.state.tags}
                            onValueAdd={(value) => this.setState(
                                (state) => ({ tags: [...state.tags, value] })
                            )}
                            onValueDelete={(value, idx) => this.setState(
                                (state) => ({ tags: [...state.tags.slice(0, idx), ...state.tags.slice(idx+1)] })
                            )}
                        />

                        <div className={classes.spacer}/>
                        <Typography variant='subtitle1'>Resources</Typography>

                        <Grid container>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label="Disk"
                                    tooltip="Amount of disk space allocated for this run.
                                    Defaults to amount of user quota left."
                                />
                                <ConfigTextInput
                                    value={this.state.disk}
                                    onValueChange={(value) => this.setState({ disk: value })}
                                    placeholder="5g"
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label="Memory"
                                    tooltip="Amount of memory allocated for this run."
                                />
                                <ConfigTextInput
                                    value={this.state.memory}
                                    onValueChange={(value) => this.setState({ memory: value })}
                                    placeholder="5g"
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label="CPUs"
                                    tooltip="Number of CPUs allocated for this run."
                                />
                                <ConfigTextInput
                                    value={this.state.cpu}
                                    onValueChange={(value) => this.setState({ cpu: value })}
                                    placeholder="1"
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label="GPUs"
                                    tooltip="Number of GPUs allocated for this run."
                                />
                                <ConfigTextInput
                                    value={this.state.gpu}
                                    onValueChange={(value) => this.setState({ gpu: value })}
                                    placeholder="1"
                                />
                            </Grid>
                            <Grid item xs={12}>
                                <ConfigLabel
                                    label="Docker Image"
                                    tooltip="Tag or digest of Docker image to serve as the
                                    virtual run environment."
                                />
                                <ConfigTextInput
                                    value={this.state.docker}
                                    onValueChange={(value) => this.setState({ docker: value })}
                                    placeholder="codalab/default-cpu:latest"
                                />
                            </Grid>
                            <Grid item xs={12}>
                                <ConfigSwitchInput
                                    value={this.state.networkAccess}
                                    onValueChange={(value) => this.setState({ networkAccess: value })}
                                />
                                <ConfigLabel
                                    label="Network Access"
                                    tooltip="Whether the bundle can open any external
                                    network ports."
                                    inline
                                />
                            </Grid>
                            <Grid item xs={12}>
                                <ConfigSwitchInput
                                    value={this.state.failedDependencies}
                                    onValueChange={(value) => this.setState({ failedDependencies: value })}
                                />
                                <ConfigLabel
                                    label="Failed Dependencies"
                                    tooltip="Whether the bundle can depend on failed/killed
                                    dependencies."
                                    inline
                                />
                            </Grid>
                        </Grid>
                    </div>
                )}
            >
                {/* Main Content ------------------------------------------------------- */}
                <Typography variant='subtitle1' gutterBottom>New Run</Typography>
                <ConfigLabel
                    label="Dependencies"
                    tooltip="Map an entire bundle or a file/directory inside to a name that
                    can be referenced in the terminal command."
                />
                <DependencyEditor
                    addDependency={(dep) => this.addDependency(dep)}
                    updateDependency={(idx, alias) => this.updateDependency(idx, alias)}
                    removeDependency={(idx) => this.removeDependency(idx)}
                    dependencies={this.state.dependencies}
                    candidates={candidates}
                />

                <div className={classes.spacer}/>
                <ConfigLabel
                    label="Command"
                    tooltip="Terminal command to run within the Docker container. It can use
                    data from other bundles by referencing the aliases specified in the
                    dependencies section."
                />
                <ConfigCodeInput
                    value={this.state.command}
                    onValueChange={(value) => this.setState({ command: value })}
                    multiline
                    placeholder="python train.py --data mydataset.txt"
                    maxRows={4}
                />
            </ConfigPanel>
        );
    }
}

const styles = (theme) => ({
    spacer: {
        marginTop: theme.spacing.larger,
    },
});

export default withStyles(styles)(NewRun);
