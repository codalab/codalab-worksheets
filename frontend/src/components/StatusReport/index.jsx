import React from 'react';
import DatePicker from 'react-datepicker';
import { withStyles } from '@material-ui/core/styles';
import Paper from '@material-ui/core/Paper';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import TableHead from '@material-ui/core/TableHead';
import { executeCommand } from '../../util/apiWrapper';
import Loading from '../Loading';
import ErrorMessage from '../worksheets/ErrorMessage';
import HelpTooltip from '../HelpTooltip';
import 'react-datepicker/dist/react-datepicker.css';

/**
 * This component renders a status report that programatically answers common
 * questions about CodaLab's user activity such as:
 *
 * How many new users joined?
 * How many users are active?
 * How many workers are up?
 * How many new bundles have been created?
 * How many bundles have failed?
 */
class StatusReport extends React.Component {
    constructor(props) {
        super(props);
        this.handleDateChange = this.handleDateChange.bind(this);
        this.state = {
            reportDate: new Date(),
            reportIsLoading: true,
            reportIsErroring: false,
            report: {
                newUserCount: '',
                activeUserCount: '',
                newBundleCount: '',
                failedBundleCount: '',
                activeWorkers: '',
            },
        };
    }

    fetchReport(date) {
        const formattedDate = date.toISOString();
        const newUserCommand = `cl uls .joined_after=${formattedDate} .count`;
        const activeUserCommand = `cl uls .active_after=${formattedDate} .count`;
        const newBundleCommand = `cl search .after=${formattedDate} .count`;
        const failedBundleCommand = `cl search .after=${formattedDate}  state=failed .count`;
        const activeWorkersCommand = `cl workers -c`;
        const uuid = this.props.worksheetUUID;
        const promises = [];
        const report = {};

        this.setState({
            reportIsLoading: true,
            reportIsErroring: false,
        });

        promises.push(
            executeCommand(newUserCommand, uuid).then((resp) => {
                report.newUserCount = resp.output;
            }),
        );
        promises.push(
            executeCommand(activeUserCommand, uuid).then((resp) => {
                report.activeUserCount = resp.output;
            }),
        );
        promises.push(
            executeCommand(newBundleCommand, uuid).then((resp) => {
                report.newBundleCount = resp.output;
            }),
        );
        promises.push(
            executeCommand(failedBundleCommand, uuid).then((resp) => {
                report.failedBundleCount = resp.output;
            }),
        );
        promises.push(
            executeCommand(activeWorkersCommand, uuid).then((resp) => {
                report.activeWorkers = resp.output;
            }),
        );
        Promise.all(promises)
            .then(() => {
                this.setState({
                    reportIsLoading: false,
                    reportIsErroring: false,
                    report,
                });
            })
            .catch(() => {
                this.setState({
                    reportIsLoading: false,
                    reportIsErroring: true,
                });
            });
    }

    handleDateChange(date) {
        this.setState({ reportDate: date });
        this.fetchReport(date);
    }

    createRowData(query, data) {
        const dataList = data.split('\n');
        return { query, data: dataList };
    }

    componentDidMount() {
        this.fetchReport(new Date()); // initial report
    }

    render() {
        const { classes } = this.props;
        const { reportIsLoading, reportIsErroring, reportDate, report } = this.state;
        const errorText = 'Error: Unable to fetch status report.';
        const helpText =
            'Use the date picker below to select your report date range. Results will reflect activity between the selected date and now.';
        const tableRows = [
            this.createRowData('New User Count', report.newUserCount),
            this.createRowData('Active User Count', report.activeUserCount),
            this.createRowData('New Bundle Count', report.newBundleCount),
            this.createRowData('Failed Bundle Count', report.failedBundleCount),
            this.createRowData('Active Worker IDs', report.activeWorkers),
        ];

        return (
            <div className={classes.reportContainer}>
                <h1>Status Report</h1>
                <div className={classes.subheading}>
                    <h4>Report Start Date</h4>
                    <HelpTooltip className={classes.tooltip} title={helpText} />
                </div>
                <DatePicker
                    selected={reportDate}
                    onChange={this.handleDateChange}
                    className={classes.datePicker}
                    popperClassName={classes.popper}
                    dateFormat={'M/d/yyyy'}
                    showPopperArrow={false}
                />
                {reportIsLoading && <Loading />}
                {reportIsErroring && <ErrorMessage message={errorText} noMargin />}
                {!reportIsLoading && !reportIsErroring && (
                    <TableContainer component={Paper}>
                        <Table>
                            <TableHead classes={{ root: classes.row }}>
                                <TableCell>
                                    Query [since {reportDate.toLocaleDateString()}]
                                </TableCell>
                                <TableCell>Data</TableCell>
                            </TableHead>
                            <TableBody>
                                {tableRows.map((row) => (
                                    <TableRow classes={{ root: classes.row }}>
                                        <TableCell>{row.query}</TableCell>
                                        <TableCell>
                                            {row.data.map((data) => (
                                                <div>{data}</div>
                                            ))}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                )}
            </div>
        );
    }
}

const styles = () => ({
    reportContainer: {
        marginBottom: 35,
    },
    subheading: {
        display: 'flex',
    },
    tooltip: {
        fontSize: 13,
        padding: 4,
    },
    datePicker: {
        marginBottom: 20,
    },
    popper: {
        zIndex: 10,
    },
    row: {
        height: 30,
    },
});

export default withStyles(styles)(StatusReport);
