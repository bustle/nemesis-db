const { join } = require('path')

require('dotenv').config({ path: join(__dirname, '../../../.env_test') })
require('./setup')
