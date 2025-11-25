// Kill any process using port 5000
const { execSync } = require('child_process');

try {
  // Try to find and kill process on port 5000
  const command = process.platform === 'win32' 
    ? 'FOR /F "tokens=5" %a IN (\'netstat -ano ^| findstr :5000\') DO taskkill /F /PID %a'
    : 'lsof -ti:5000 | xargs kill -9';
  
  console.log('Checking for processes on port 5000...');
  execSync(command, { stdio: 'ignore' });
  console.log('Cleared port 5000');
} catch (error) {
  // Port is already free or no process found
  console.log('Port 5000 is available');
}
