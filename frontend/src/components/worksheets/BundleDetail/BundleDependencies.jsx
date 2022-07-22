import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import { shorten_uuid } from '../../../util/worksheet_utils';

/**
 * This component renders bundle dependencies in an MUI table.
 */
class BundleDependencies extends React.PureComponent {
    constructor(props) {
        super(props);
    }

    render() {
        const { bundle } = this.props;
        const dependencies = bundle.dependencies.value;

        if (!dependencies.length) {
            return <div>{'<none>'}</div>;
        }

        const dependenciesTable = [];
        dependencies.forEach((dep, i) => {
            const depBundleUrl = '/bundles/' + dep.parent_uuid;
            dependenciesTable.push(
                <TableRow key={dep.parent_uuid + i}>
                    <TableCell>
                        {dep.child_path}
                        <br /> &rarr; {dep.parent_name}(
                        <a href={depBundleUrl} target='_blank'>
                            {shorten_uuid(dep.parent_uuid)}
                        </a>
                        ){dep.parent_path ? '/' + dep.parent_path : ''}
                    </TableCell>
                </TableRow>,
            );
        });

        return (
            <Table>
                <TableBody>{dependenciesTable}</TableBody>
            </Table>
        );
    }
}

export default BundleDependencies;
