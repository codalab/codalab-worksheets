// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';
import DeleteIcon from '@material-ui/icons/Delete';
import Select from 'react-select';
import Mousetrap from '../../../util/ws_mousetrap_fork';
import '../../../util/mousetrap_global_bind';

import ConfigPanel, {
    ConfigLabel,
    ConfigTextInput,
    ConfigChipInput,
    ConfigCodeInput,
    ConfigSwitchInput,
} from '../ConfigPanel';

import { shorten_uuid, buildTerminalCommand } from '../../../util/worksheet_utils';

import { executeCommand } from '../../../util/apiWrapper';

type Bundle = { name: string, uuid: string, path?: string };
type Dependency = { target: Bundle, alias: string };

class _DependencyEditor extends React.Component<{
    /** JSS styling object. */
    classes: {},

    /** Functions to update state. */
    addDependency: (Bundle) => void,
    updateDependency: (number, string) => void,
    removeDependency: (number) => void,
    addSubpath: (number, string) => void,

    /** Candidate dependencies. */
    dependencies: Dependency[],
    candidates?: Bundle[],
}> {
    static defaultProps = {
        dependencies: [],
        candidates: [],
    };
    render() {
        const {
            classes,
            dependencies,
            candidates,
            addDependency,
            updateDependency,
            removeDependency,
            addSubpath,
        } = this.props;

        const subpathInputProps = {
            style: {
                fontSize: 14,
            },
            disableUnderline: true,
        };

        return (
            <Grid container direction='column' className={classes.container}>
                {/* Existing dependencies ------------------------------------------------------ */}
                {dependencies.map((dep, idx) => (
                    <Grid item container direction='row' key={idx}>
                        <Grid item xs={4}>
                            <Typography variant='body1'>
                                {`${dep.target.name} (${shorten_uuid(dep.target.uuid)})`}
                            </Typography>
                        </Grid>
                        <Grid item xs={2}>
                            <div className={classes.subpathContainer}>
                                <Typography variant='body1' style={{ marginTop: 2 }}>
                                    /
                                </Typography>
                                <ConfigTextInput
                                    value={dep.target.path}
                                    placeholder='subpath'
                                    onValueChange={(alias) => addSubpath(idx, alias)}
                                    customInputProps={subpathInputProps}
                                />
                            </div>
                        </Grid>
                        <Grid item xs={1} container justify='center'>
                            <Typography variant='body2'>as</Typography>
                        </Grid>
                        <Grid item xs={3}>
                            <ConfigTextInput
                                value={dep.alias}
                                onValueChange={(alias) => updateDependency(idx, alias)}
                            />
                        </Grid>
                        <Grid item xs={1} container justify='center'>
                            <IconButton onClick={() => removeDependency(idx)}>
                                <DeleteIcon fontSize='small' />
                            </IconButton>
                        </Grid>
                    </Grid>
                ))}

                {/* New dependency ------------------------------------------------------------- */}
                <Grid item container direction='row' key={-1}>
                    <Grid item xs={4}>
                        <Select
                            options={candidates.map((bundle) => ({
                                label: `${bundle.name} (${shorten_uuid(bundle.uuid)})`,
                                value: bundle,
                            }))}
                            value=''
                            onChange={(option) => addDependency(option.value)}
                            placeholder='target'
                            noOptionsMessage={() => 'No matching bundles'}
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
                    <Grid item xs={2} />
                    <Grid item xs={1} container justify='center'>
                        <Typography variant='body2'>as</Typography>
                    </Grid>
                    <Grid item xs={3}>
                        <ConfigTextInput disabled value='alias' />
                    </Grid>
                </Grid>
            </Grid>
        );
    }
}
const DependencyEditor = withStyles((theme) => ({
    container: {
        paddingBottom: theme.spacing.large,
    },
    subpathContainer: {
        display: 'flex',
        backgroundColor: '#EFF1F3',
        height: 28,
    },
}))(_DependencyEditor);

const kDefaultCpu = 1;
const kDefaultGpu = 0;
const kDefaultDockerCpu = 'codalab/default-cpu:latest';
const kDefaultDockerGpu = 'codalab/default-gpu:latest';
const kDefaultMemory = '4g';

class NewRun extends React.Component<
    {
        /** JSS styling object. */
        classes: {},

        /** Worksheet info. */
        ws: {},
        reloadWorksheet: () => void,
        onSubmit: () => void,
        defaultRun: {},
    },
    {
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
    },
> {
    static defaultProps: {
        onSubmit: () => undefined,
        reloadWorksheet: () => undefined,
        defaultRun: {},
    };
    defaultConfig = {
        dependencies: [],
        command: '',
        name: '',
        description: '',
        tags: [],
        disk: '',
        memory: kDefaultMemory,
        cpu: kDefaultCpu,
        gpu: kDefaultGpu,
        docker: kDefaultDockerCpu,
        networkAccess: true,
        failedDependencies: false,
        queue: '',
    };

    /**
     * Constructor.
     * @param props
     */
    constructor(props) {
        super(props);
        this.state = {
            ...Object.assign(this.defaultConfig, props.defaultRun),
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
     *     List index of dependency to be removed.
     */
    removeDependency(idx: number) {
        const { dependencies } = this.state;
        dependencies.splice(idx, 1);
        this.setState({ dependencies });
    }

    /**
     * Add a subpath to an existing dependency
     * @param idx
     *     Index of dependency to add subpath to
     * @param subpath
     *     Subpath to be added to dependency
     */
    addSubpath(idx: number, subpath: string) {
        const { dependencies } = this.state;
        dependencies[idx].target.path = subpath;
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
            queue,
            exclude_patterns,
        } = this.state;
        const { after_sort_key } = this.props;

        let args = ['run'];

        if (after_sort_key || after_sort_key === 0) args.push(`-a ${after_sort_key}`);
        if (name) args.push(`--name=${name}`);
        if (description) args.push(`--description=${description}`);
        if (tags) args.push(`--tags=${tags.map((tag) => `'${tag}'`).join(',')}`);
        if (disk) args.push(`--request-disk=${disk}`);
        if (memory) args.push(`--request-memory=${memory}`);
        if (cpu) args.push(`--request-cpus=${cpu}`);
        if (gpu) args.push(`--request-gpus=${gpu}`);
        if (docker) args.push(`--request-docker-image=${docker}`);
        if (queue) args.push(`--request-queue=${queue}`);
        if (networkAccess) args.push(`--request-network`);
        if (failedDependencies) args.push(`--allow-failed-dependencies`);

        for (let dep of dependencies) {
            const key = dep.alias;
            let value = dep.target.uuid;
            if (dep.target.path) value += '/' + dep.target.path;
            args.push(key + ':' + value);
        }

        if (command) args.push(command);

        // exclude_patterns can take a list of arguments, so put it at the end
        if (exclude_patterns) {
            args.push(`--exclude-patterns`);
            for (let i = 0; i < exclude_patterns.length; i++) {
                args.push(`${exclude_patterns[i]}`);
            }
        }

        return buildTerminalCommand(args);
    }

    runCommand() {
        const cmd = this.getCommand();
        if (cmd) {
            executeCommand(cmd, this.props.ws.info.uuid).then(() => {
                const moveIndex = true;
                const param = { moveIndex };
                this.props.reloadWorksheet(undefined, undefined, param);
            });
        }
    }

    shortcuts() {
        Mousetrap.bindGlobal(['escape'], () => this.props.onSubmit());
    }

    /**
     * Render.
     */
    render() {
        const { classes, ws } = this.props;
        this.shortcuts();
        let candidates: Bundle[] = [];
        if (ws && ws.info && ws.info.blocks) {
            ws.info.blocks.forEach((item) => {
                if (item.bundles_spec && item.bundles_spec.bundle_infos) {
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
                buttons={
                    <div>
                        <Button
                            variant='text'
                            color='primary'
                            onClick={() => this.props.onSubmit()}
                        >
                            Cancel
                        </Button>
                        <Button
                            variant='contained'
                            color='primary'
                            onClick={() => {
                                this.runCommand();
                                this.props.onSubmit();
                            }}
                        >
                            Confirm
                        </Button>
                    </div>
                }
                sidebar={
                    <div>
                        <ConfigLabel
                            label='Name'
                            tooltip='Short name (not necessarily unique) to provide an
                            easy, human-readable way to reference this bundle (e.g as a
                            dependency). May only use alphanumeric characters and dashes.'
                        />
                        <ConfigTextInput
                            value={this.state.name}
                            onValueChange={(value) => this.setState({ name: value })}
                            optional
                        />

                        <ConfigLabel
                            label='Description'
                            tooltip='Text description or notes about this bundle.'
                            optional
                        />
                        <ConfigTextInput
                            value={this.state.description}
                            onValueChange={(value) => this.setState({ description: value })}
                            multiline
                            maxRows={3}
                        />

                        <ConfigLabel
                            label='Tags'
                            tooltip='Keywords that can be used to search for and categorize
                            this bundle.'
                            optional
                        />
                        <ConfigChipInput
                            values={this.state.tags}
                            onValueAdd={(value) =>
                                this.setState((state) => ({ tags: [...state.tags, value] }))
                            }
                            onValueDelete={(value, idx) =>
                                this.setState((state) => ({
                                    tags: [
                                        ...state.tags.slice(0, idx),
                                        ...state.tags.slice(idx + 1),
                                    ],
                                }))
                            }
                        />
                        <div className={classes.spacer} />
                        <Typography variant='subtitle1'>Resources</Typography>

                        <Grid container>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label='Disk'
                                    tooltip='Amount of disk space allocated for this run.
                                    If left blank, the default is all remaining user quota.'
                                />
                                <ConfigTextInput
                                    value={this.state.disk}
                                    onValueChange={(value) => this.setState({ disk: value })}
                                    placeholder={`${'disk space (g)'}`}
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label='Memory'
                                    tooltip='Amount of memory allocated for this run.'
                                />
                                <ConfigTextInput
                                    value={this.state.memory}
                                    onValueChange={(value) => this.setState({ memory: value })}
                                    placeholder={`${kDefaultMemory}`}
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label='CPUs'
                                    tooltip='Number of CPUs allocated for this run.'
                                />
                                <ConfigTextInput
                                    value={this.state.cpu}
                                    onValueChange={(value) => {
                                        const cpu = parseInt(value);
                                        if (isNaN(cpu)) {
                                            this.setState({ cpu: value });
                                            return;
                                        }
                                        this.setState({ cpu: cpu });
                                    }}
                                    placeholder={`${kDefaultCpu}`}
                                />
                            </Grid>
                            <Grid item xs={6}>
                                <ConfigLabel
                                    label='GPUs'
                                    tooltip='Number of GPUs allocated for this run.'
                                />
                                <ConfigTextInput
                                    value={this.state.gpu}
                                    onValueChange={(value) => {
                                        const gpu = parseInt(value);
                                        if (isNaN(gpu)) {
                                            this.setState({ gpu: value });
                                            return;
                                        }
                                        this.setState({ gpu: gpu });

                                        if (gpu > 0 && this.state.docker === kDefaultDockerCpu) {
                                            this.setState({ docker: kDefaultDockerGpu });
                                        } else if (
                                            gpu === 0 &&
                                            this.state.docker === kDefaultDockerGpu
                                        ) {
                                            this.setState({ docker: kDefaultDockerCpu });
                                        }
                                    }}
                                    placeholder={`${kDefaultGpu}`}
                                />
                            </Grid>
                            <Grid item xs={12}>
                                <ConfigLabel
                                    label='Docker Image'
                                    tooltip='Tag or digest of Docker image to serve as the
                                    virtual run environment.'
                                />
                                <ConfigTextInput
                                    value={this.state.docker}
                                    onValueChange={(value) => this.setState({ docker: value })}
                                    placeholder={`${kDefaultDockerCpu}`}
                                />
                            </Grid>
                            <ConfigLabel
                                label='Queue'
                                tooltip="Tag of the queue, this will add '--request-queue {input}' to the run"
                                optional
                            />
                            <ConfigTextInput
                                value={this.state.queue}
                                onValueChange={(value) => {
                                    this.setState({ queue: value });
                                }}
                                placeholder={''}
                            />
                            <Grid item xs={12}>
                                <ConfigSwitchInput
                                    value={this.state.failedDependencies}
                                    onValueChange={(value) =>
                                        this.setState({ failedDependencies: value })
                                    }
                                />
                                <ConfigLabel
                                    label='Failed Dependencies'
                                    tooltip='Whether the bundle can depend on failed/killed
                                    dependencies.'
                                    inline
                                />
                            </Grid>
                        </Grid>
                    </div>
                }
            >
                {/* Main Content ------------------------------------------------------- */}
                <Typography variant='subtitle1' gutterBottom>
                    New Run
                </Typography>
                <ConfigLabel
                    label='Dependencies'
                    tooltip='Map an entire bundle or a file/directory inside to a name that
                    can be referenced in the terminal command.'
                />
                <DependencyEditor
                    addDependency={(dep) => this.addDependency(dep)}
                    updateDependency={(idx, alias) => this.updateDependency(idx, alias)}
                    removeDependency={(idx) => this.removeDependency(idx)}
                    addSubpath={(idx, subpath) => this.addSubpath(idx, subpath)}
                    dependencies={this.state.dependencies}
                    candidates={candidates}
                />

                <div className={classes.spacer} />
                <ConfigLabel
                    label='Command'
                    tooltip='Terminal command to run within the Docker container. It can use
                    data from other bundles by referencing the aliases specified in the
                    dependencies section.'
                />
                <ConfigCodeInput
                    value={this.state.command}
                    onValueChange={(value) => this.setState({ command: value })}
                    multiline
                    autoFocus
                    placeholder='python train.py --data mydataset.txt'
                    maxRows={4}
                    onKeyDown={(e) => {
                        if (e.keyCode === 13 && !e.shiftKey) {
                            // if strictly enter key is pressed
                            e.preventDefault();
                            this.runCommand();
                            this.props.onSubmit();
                        }
                    }}
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
