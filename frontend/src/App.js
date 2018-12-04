import * as React from 'react';

class App extends React.Component {
    state = {
        name: 'frontend',
    };

    render() {
        return (
            <div className='App'>
                <h1>Welcome to {this.state.name}</h1>
            </div>
        );
    }
}

const x: number = "str";
x + 1;
console.log(x);
export default App;
