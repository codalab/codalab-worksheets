// @flow
import * as React from 'react';

import { withStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Button from '@material-ui/core/Button';
import AddIcon from '@material-ui/icons/PlayCircleFilled';
import HelpIcon from '@material-ui/icons/Help';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import TextField from '@material-ui/core/TextField';

import ConfigurationPanel from '../ConfigurationPanel';
// import Search from './Search';
// import DependencyMap from './DependencyMap';
// import Configuration from './Configuration';
// import RunInfo from './RunInfo';
// import CommandInput from './CommandInput';
// import './NewRun.css';


class NewRun extends React.Component<{
    /** JSS styling object. */
    classes: {},
}, {
    /** Whether to show draw at bottom of the screen. */
    isDrawerVisible: boolean,

    /** Displayed as "[target] as [alias]", e.g { target: "foo(0x41a160)", alias: "foo" }. */
    dependencies: {
        target: string,
        alias: string,
    }[],

    /** Displayed as chips that can be deleted. */
    tags: string[],
}> {

    /**
     * Constructor.
     * @param props
     */
    constructor(props) {
        super(props);
        this.state = {
            isDrawerVisible: false,
            dependencies: [],
            tags: [],
        };
    }

    /**
     * @param isVisible
     *     Whether the drawer should be opened (true) or closed (false).
     */
    toggleDrawer(isVisible: boolean) {
        this.setState({ isDrawerVisible: isVisible });
    }

    /**
     * Add a new dependency to the list, with the alias defaulting to the bundle name.
     * @param bundle
     *     Bundle object of target.
     */
    addDependency(bundle: { name: string, uuid: string }) {
        if (!bundle) return;
        const { dependencies } = this.state;
        dependencies.push({
            target: `${bundle.name}(${bundle.uuid})`,
            alias: bundle.name,
        });
        this.setState({ dependencies });
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

    /**
     * @param tag
     */
    addTag(tag: string) {
        if (!tag) return;
        const { tags } = this.state;
        tags.push(tag);
        this.setState({ tags });
    }

    /**
     * @param idx
     */
    removeTag(idx: number) {
        const { tags } = this.state;
        tags.splice(idx, 1);
        this.setState({ tags });
    }

    /**
     * Render.
     */
    render() {
        const { classes } = this.props;
        const { isDrawerVisible, dependencies, tags, commandInput } = this.state;

        return (
            <div>
                {/* Button ===================================================================== */}
                <Button
                    variant="contained"
                    size="medium"
                    color="primary"
                    aria-label="Add"
                    onClick={ () => this.toggleDrawer(true) }
                >
                    <AddIcon className={classes.buttonIcon} />
                    New Run
                </Button>

                {/* Drawer ===================================================================== */}
                <Drawer
                    anchor="bottom"
                    open={ isDrawerVisible }
                    onClose={ () => this.toggleDrawer(false) }
                    classes={ { paper: classes.drawer } }
                >
                    <ConfigurationPanel
                        buttons={(
                            <div>
                                <Button variant='text' color='primary'>Cancel</Button>
                                <Button variant='contained' color='primary'>Confirm</Button>
                            </div>
                        )}
                        sidebar={(
                            <div>
                                <Typography variant='subtitle1'>Information</Typography>

                                <Typography variant='subtitle1'>Resources</Typography>
                                <Typography variant='subtitle2'>Disk</Typography>
                                <Typography variant='subtitle2'>Memory</Typography>
                                <Typography variant='subtitle2'>CPUs</Typography>
                                <Typography variant='subtitle2'>GPUs</Typography>
                                <Typography variant='subtitle2'>Docker Image</Typography>
                                <Typography variant='subtitle2'>Allow Network</Typography>
                            </div>
                        )}
                    >
                        <Typography variant='subtitle1' gutterBottom>Dependencies</Typography>


                        <Typography variant='subtitle1' gutterBottom>Command</Typography>
                        <TextField
                            className={classes.commandInput}
                            value={commandInput}
                            onChange={(e) => this.setState({ commandInput: e.target.value })}
                            margin="dense"
                            variant="outlined"
                        />
                    </ConfigurationPanel>
                </Drawer>
            </div>
        );
    }
}

{/*<div className={classes.outerDiv}>*/}
    {/*<div*/}
        {/*style={ {*/}
            {/*display: 'flex',*/}
            {/*flexDirection: 'row',*/}
            {/*justifyContent: 'space-between',*/}
            {/*flex: 1,*/}
            {/*zIndex: 10,*/}
        {/*} }*/}
    {/*>*/}
        {/*<div*/}
            {/*style={ { flex: 1, marginRight: 16 } }*/}
        {/*>*/}
            {/*<div className="row sectionTitle">*/}
                {/*<div style={ { marginRight: 16 } }>*/}
                    {/*Dependencies*/}
                {/*</div>*/}
                {/*<Tooltip*/}
                    {/*title={*/}
                        {/*<Typography variant="caption" style={ {*/}
                            {/*color: 'white',*/}
                        {/*} }>*/}
                            {/*Map an entire bundle or file/directory inside a bundle to a name,*/}
                            {/*which can be referenced in the python command.*/}
                        {/*</Typography>*/}
                    {/*}*/}
                {/*>*/}
                    {/*<HelpIcon />*/}
                {/*</Tooltip>*/}
            {/*</div>*/}
            {/*<table>*/}
                {/*<tbody>*/}
                {/*{*/}
                    {/*dependencies*/}
                        {/*.map((ele, idx) => <DependencyMap*/}
                            {/*key={ idx }*/}
                            {/*name={ ele.name }*/}
                            {/*bundle={ ele.bundle }*/}
                            {/*onChange={*/}
                                {/*(event) => this.updateDependency(idx, event)*/}
                            {/*}*/}
                            {/*onRemove={*/}
                                {/*this.removeDependency(idx)*/}
                            {/*}*/}
                        {/*/>)*/}
                {/*}*/}
                {/*</tbody>*/}
            {/*</table>*/}
            {/*<Search*/}
                {/*searchHandler={ this.addDependency }*/}
            {/*/>*/}
        {/*</div>*/}
        {/*<Configuration*/}
            {/*handleChange={ (name, e) => this.handleChange(name, e) }*/}
            {/*handleCheck={ (name, e) => this.handleCheck(name, e) }*/}
            {/*network={ network }*/}
            {/*failedOkay={ failedOkay }*/}
        {/*/>*/}
        {/*<RunInfo*/}
            {/*handleChange={ this.handleChange }*/}
            {/*addTag={ this.addTag }*/}
            {/*removeTag={ this.removeTag }*/}
            {/*tags={ tags }*/}
        {/*/>*/}
    {/*</div>*/}
    {/*<CommandInput />*/}
{/*</div>*/}

const styles = (theme) => ({
    buttonIcon: {
        marginRight: theme.spacing.large
    },
    drawer: {
        minHeight: '50vh',
        width: '75vw',
        marginLeft: 'auto',
        marginRight: 'auto',
        borderTopLeftRadius: theme.spacing.unit,
        borderTopRightRadius: theme.spacing.unit,
    },
    commandInput: {
        fontFamily: 'monospace',
    },
    outerDiv: {
        display: 'flex',
        flexDirection: 'column',
        flex: 1,
        padding: 16,
        justifyContent: 'space-between',
    },
});

export default withStyles(styles)(NewRun);
