<img alttext="COVID Green Logo" src="https://raw.githubusercontent.com/lfph/artwork/master/projects/covidgreen/stacked/color/covidgreen-stacked-color.png" width="300" />

# AWS Lambda Function Implementations

This repository contains various lambdas useful within the Covid Green system.

## Local Development: Running the Lambdas

There are a number of handy commands you can run to help with development.
​
| Command | Action |
| --- | --- |
| `npm run create:env` | Create new .env file |
| `npm test` | Execute all lambdas in a limited testing mode - should at least find compile errors |
| `npm run test:exposures` | Run test for exposures |
| `npm run test:settings` | Run test for settings |
| `npm run test:authorizer` | Run test for authorizer. The `authorizationToken` value is a JWT signed with key equal to string `secret` |
| `npm run test:token:metrics` | Run test for token specific to metrics |
| `npm run test:token:push` | Run test for token specific to push |
| `npm run test:token:register` | Run test for token specific to register |
| `npm run test:cleanup` | Run cleanup for local env |
| `npm run lint` | Run eslint |
| `npm run lint:fix` | Run eslint in fix mode |

## Team

### Lead Maintainers

* @colmharte - Colm Harte <colm.harte@nearform.com>
* @jasnell - James M Snell <jasnell@gmail.com>
* @aspiringarc - Gar Mac Críosta <gar.maccriosta@hse.ie>

### Core Team

* @ShaunBaker - Shaun Baker <shaun.baker@nearform.com>
* @floridemai - Paul Negrutiu <paul.negrutiu@nearform.com>
* @jackdclark - Jack Clark <jack.clark@nearform.com>
* @andreaforni - Andrea Forni <andrea.forni@nearform.com>
* @jackmurdoch - Jack Murdoch <jack.murdoch@nearform.com>

### Contributors

* @dennisgove - Dennis Gove <dpgove@gmail.com>

### Past Contributors

## Hosted By

<a href="https://www.lfph.io"><img alttext="Linux Foundation Public Health Logo" src="https://raw.githubusercontent.com/lfph/artwork/master/lfph/stacked/color/lfph-stacked-color.svg" width="200"></a>

[Linux Foundation Public Health](https://www.lfph.io)

## Acknowledgements

<a href="https://www.hse.ie"><img alttext="HSE Ireland Logo" src="https://www.hse.ie/images/hse.jpg" width="200" /></a><a href="https://nearform.com"><img alttext="NearForm Logo" src="https://openjsf.org/wp-content/uploads/sites/84/2019/04/nearform.png" width="400" /></a>

## License

Copyright (c) 2020 HSEIreland
Copyright (c) The COVID Green Contributors

[Licensed](LICENSE) under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
