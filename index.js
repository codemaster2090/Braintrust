const fs = require('fs');
const path = require('path');

// Load the exported data
const dataFile = process.argv[2] || 'braintrust_developers.json';
let rawData;

try {
    rawData = fs.readFileSync(dataFile, 'utf8');
    console.log(`✅ Loaded data from ${dataFile}`);
} catch (error) {
    console.error(`❌ Error loading file: ${error.message}`);
    console.log(`Usage: node filter_names.js <filename.json>`);
    process.exit(1);
}

const allData = JSON.parse(rawData);
console.log(`📊 Total states: ${Object.keys(allData).length}`);

// Count total developers before filtering
let totalBefore = 0;
for (const state in allData) {
    totalBefore += allData[state].developers?.length || 0;
}
console.log(`👥 Total developers before filtering: ${totalBefore}`);

// Comprehensive list of Asian origin surnames (common East, South, Southeast Asian)
const asianSurnames = new Set([
    // Chinese
    'Wang', 'Li', 'Zhang', 'Liu', 'Chen', 'Yang', 'Huang', 'Zhao', 'Wu', 'Zhou',
    'Xu', 'Sun', 'Ma', 'Zhu', 'Lin', 'Guo', 'He', 'Gao', 'Zheng', 'Liang',
    'Xie', 'Song', 'Tang', 'Xu', 'Deng', 'Xiao', 'Feng', 'Zeng', 'Cheng', 'Cai',
    'Peng', 'Pan', 'Yuan', 'Yu', 'Dong', 'Su', 'Lu', 'Jiang', 'Wei', 'Shen',
    'Yao', 'Zou', 'Xiong', 'Jin', 'Wan', 'Qin', 'Xue', 'Gu', 'Tan', 'Cui',
    'Shi', 'Hou', 'Tian', 'Meng', 'Bai', 'Long', 'Qiao', 'Gu', 'Gong', 'Hong',
    
    // Korean
    'Kim', 'Lee', 'Park', 'Choi', 'Jung', 'Kang', 'Cho', 'Yoon', 'Jang', 'Lim',
    'Han', 'Oh', 'Seo', 'Shin', 'Kwon', 'Hwang', 'Ahn', 'Song', 'Hong', 'Yoo',
    'Nam', 'Bae', 'Baek', 'Chun', 'Chu', 'Eom', 'Heo', 'Hyeon', 'Jeon', 'Ji',
    'Jin', 'Ko', 'Kook', 'Kweon', 'Kyung', 'Ma', 'Min', 'Moon', 'Na', 'No',
    'Pyo', 'Ryu', 'Sim', 'Sohn', 'Tak', 'Won', 'Yang', 'Yeom', 'Yim', 'Yun',
    
    // Japanese
    'Sato', 'Suzuki', 'Takahashi', 'Tanaka', 'Watanabe', 'Ito', 'Yamamoto', 'Nakamura',
    'Kobayashi', 'Kato', 'Yoshida', 'Yamada', 'Sasaki', 'Yamaguchi', 'Matsumoto', 'Inoue',
    'Kimura', 'Hayashi', 'Shimizu', 'Fujita', 'Ikeda', 'Hashimoto', 'Nakayama', 'Ishikawa',
    'Nakano', 'Maeda', 'Ogawa', 'Goto', 'Okada', 'Fukuda', 'Takada', 'Fujii', 'Sakai',
    'Nishimura', 'Hasegawa', 'Murakami', 'Mori', 'Abe', 'Harada', 'Taniguchi', 'Yamashita',
    'Ueda', 'Nakagawa', 'Miyamoto', 'Kondo', 'Ishii', 'Saito', 'Endo', 'Aoki', 'Fujiwara',
    
    // Vietnamese
    'Nguyen', 'Tran', 'Le', 'Pham', 'Hoang', 'Huynh', 'Phan', 'Vu', 'Vo', 'Dang',
    'Bui', 'Do', 'Ngo', 'Duong', 'Ly', 'Truong', 'Dinh', 'Chau', 'Cao', 'Diep',
    'Ha', 'Hoa', 'Huu', 'Khuong', 'Lam', 'Luu', 'Mai', 'Nghiem', 'Quach', 'Thach',
    'Thai', 'Thuy', 'To', 'Ton', 'Trinh', 'Tu', 'Ung', 'Van', 'Vinh', 'Xuan',
    
    // Filipino
    'Santos', 'Reyes', 'Cruz', 'Garcia', 'Mendoza', 'Fernandez', 'Torres', 'Gonzalez',
    'Lopez', 'Rivera', 'Martinez', 'Rodriguez', 'Aquino', 'Castillo', 'Dela Cruz',
    'Diaz', 'Francisco', 'Lazaro', 'Manuel', 'Morales', 'Navarro', 'Perez', 'Ramos',
    'Ramirez', 'Sanchez', 'Santiago', 'Silva', 'Soriano', 'Valdez', 'Vargas', 'Villanueva',
    
    // Indian
    'Patel', 'Sharma', 'Kumar', 'Singh', 'Gupta', 'Reddy', 'Mehta', 'Shah', 'Verma',
    'Agarwal', 'Choudhury', 'Das', 'Jain', 'Khan', 'Malhotra', 'Mishra', 'Prasad',
    'Roy', 'Saxena', 'Sinha', 'Thakur', 'Yadav', 'Bose', 'Chatterjee', 'Ganguly',
    'Kapoor', 'Khanna', 'Nair', 'Pillai', 'Rao', 'Seth', 'Trivedi', 'Venkatesh',
    'Anand', 'Bansal', 'Chopra', 'Dhawan', 'Grover', 'Joshi', 'Kaur', 'Malik',
    'Narang', 'Oberoi', 'Puri', 'Rana', 'Sehgal', 'Tandon', 'Vohra', 'Walia',
    
    // Pakistani/Bangladeshi
    'Ahmed', 'Ali', 'Akhtar', 'Begum', 'Hasan', 'Hussain', 'Iqbal', 'Khan',
    'Malik', 'Mirza', 'Rahman', 'Rana', 'Siddiqui', 'Usman', 'Zafar', 'Chaudhry',
    
    // Thai, Cambodian, Lao, Burmese
    'Chai', 'Suk', 'Vong', 'Phan', 'Seng', 'Sok', 'Ly', 'Thach', 'Heng', 'Chea',
    'Soth', 'Pheng', 'Sim', 'Lim', 'Nguon', 'Chin', 'Yee', 'Saechao', 'Saetern',
    
    // Common Asian first names patterns
    'Wei', 'Jian', 'Ming', 'Feng', 'Hong', 'Hui', 'Jun', 'Lei', 'Min', 'Ping',
    'Qiang', 'Tao', 'Xin', 'Yong', 'Zhong', 'Chen', 'Lin', 'Hao', 'Kai', 'Li'
]);

// Names that are typically female (common English/American female names)
const femaleNames = new Set([
    'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica',
    'Sarah', 'Karen', 'Nancy', 'Lisa', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol',
    'Ruth', 'Sharon', 'Michelle', 'Laura', 'Sarah', 'Kimberly', 'Deborah', 'Jessica',
    'Shirley', 'Cynthia', 'Angela', 'Melissa', 'Brenda', 'Amy', 'Anna', 'Rebecca',
    'Virginia', 'Kathleen', 'Pamela', 'Martha', 'Debra', 'Amanda', 'Stephanie', 'Carolyn',
    'Christine', 'Marie', 'Janet', 'Catherine', 'Frances', 'Ann', 'Joyce', 'Diane',
    'Alice', 'Julie', 'Heather', 'Teresa', 'Doris', 'Gloria', 'Evelyn', 'Jean', 'Cheryl',
    'Mildred', 'Katherine', 'Joan', 'Ashley', 'Judith', 'Rose', 'Janice', 'Kelly',
    'Nicole', 'Judy', 'Christina', 'Kathy', 'Theresa', 'Beverly', 'Denise', 'Tammy',
    'Irene', 'Jane', 'Lori', 'Rachel', 'Marilyn', 'Andrea', 'Kathryn', 'Louise',
    'Sara', 'Anne', 'Jacqueline', 'Wanda', 'Bonnie', 'Julia', 'Ruby', 'Lois', 'Tina',
    'Phyllis', 'Norma', 'Paula', 'Diana', 'Annie', 'Lillian', 'Emily', 'Robin', 'Peggy',
    'Crystal', 'Gladys', 'Rita', 'Dawn', 'Connie', 'Florence', 'Tracy', 'Edna', 'Tiffany',
    'Carmen', 'Rosa', 'Cindy', 'Grace', 'Wendy', 'Victoria', 'Edith', 'Kim', 'Sherry',
    'Sylvia', 'Josephine', 'Thelma', 'Shannon', 'Sheila', 'Ethel', 'Ellen', 'Elaine',
    'Marjorie', 'Carrie', 'Charlotte', 'Monica', 'Esther', 'Pauline', 'Emma', 'Juanita',
    'Anita', 'Rhonda', 'Hazel', 'Amber', 'Eva', 'Debbie', 'April', 'Leslie', 'Clara',
    'Lucille', 'Jamie', 'Joanne', 'Eleanor', 'Valerie', 'Danielle', 'Megan', 'Alicia',
    'Suzanne', 'Michele', 'Gail', 'Bertha', 'Darlene', 'Veronica', 'Jill', 'Erin',
    'Geraldine', 'Lauren', 'Cathy', 'Joann', 'Lorraine', 'Lynn', 'Sally', 'Regina',
    'Erica', 'Beatrice', 'Dolores', 'Bernice', 'Audrey', 'Yvonne', 'Annette', 'June',
    'Samantha', 'Marion', 'Dana', 'Stacy', 'Ana', 'Renee', 'Ida', 'Vivian', 'Roberta',
    'Holly', 'Brittany', 'Melanie', 'Loretta', 'Yolanda', 'Jeanette', 'Laurie', 'Katie',
    'Kristen', 'Vanessa', 'Alma', 'Sue', 'Elsie', 'Beth', 'Jeanne', 'Vicki', 'Carla',
    'Tara', 'Rosemary', 'Eileen', 'Terri', 'Gertrude', 'Lucy', 'Tonya', 'Ella', 'Stacey',
    'Wilma', 'Gina', 'Kristin', 'Jessie', 'Natalie', 'Agnes', 'Vera', 'Willie', 'Charlene',
    'Bessie', 'Delores', 'Melinda', 'Pearl', 'Arlene', 'Maureen', 'Colleen', 'Allison',
    'Tamara', 'Joy', 'Georgia', 'Constance', 'Lillie', 'Claudia', 'Jackie', 'Marcia',
    'Tanya', 'Nellie', 'Minnie', 'Marlene', 'Heidi', 'Glenda', 'Lydia', 'Viola', 'Courtney',
    'Marian', 'Stella', 'Caroline', 'Dora', 'Jo', 'Vickie', 'Mattie', 'Maxine', 'Ima',
    'Lula', 'Millie', 'Christy', 'Adriana', 'Priscilla', 'Margaret', 'Sophia', 'Olivia',
    'Emma', 'Ava', 'Isabella', 'Mia', 'Amelia', 'Harper', 'Evelyn', 'Abigail', 'Emily',
    'Elizabeth', 'Mila', 'Ella', 'Avery', 'Sofia', 'Camila', 'Aria', 'Scarlett', 'Victoria',
    'Madison', 'Luna', 'Grace', 'Chloe', 'Penelope', 'Layla', 'Riley', 'Zoey', 'Nora',
    'Lily', 'Eleanor', 'Hannah', 'Lillian', 'Addison', 'Aubrey', 'Ellie', 'Stella',
    'Natalie', 'Zoe', 'Leah', 'Hazel', 'Violet', 'Aurora', 'Savannah', 'Audrey', 'Brooklyn',
    'Bella', 'Claire', 'Skylar', 'Lucy', 'Paisley', 'Everly', 'Anna', 'Caroline', 'Nova',
    'Genesis', 'Emilia', 'Kennedy', 'Samantha', 'Maya', 'Willow', 'Kinsley', 'Naomi',
    'Aaliyah', 'Elena', 'Sarah', 'Ariana', 'Allison', 'Gabriella', 'Alice', 'Madelyn',
    'Cora', 'Ruby', 'Eva', 'Serenity', 'Autumn', 'Adeline', 'Hailey', 'Gianna', 'Valentina',
    'Isla', 'Eliana', 'Quinn', 'Nevaeh', 'Ivy', 'Sadie', 'Piper', 'Lydia', 'Alexa',
    'Josephine', 'Emery', 'Julia', 'Delilah', 'Arianna', 'Vivian', 'Kaylee', 'Sophie',
    'Brielle', 'Madeline', 'Peyton', 'Rylee', 'Clara', 'Hadley', 'Melanie', 'Mackenzie',
    'Reagan', 'Adalynn', 'Liliana', 'Aubree', 'Jade', 'Katherine', 'Isabelle', 'Suri'
]);

// Function to detect if a name is of Asian origin
function isAsianOrigin(firstName, lastName) {
    const upperFirstName = firstName?.toUpperCase() || '';
    const upperLastName = lastName?.toUpperCase() || '';
    
    // Check last name against Asian surnames
    for (const asianName of asianSurnames) {
        if (upperLastName === asianName.toUpperCase()) {
            return true;
        }
        // Check for partial matches (some Asian names might have suffixes)
        if (upperLastName.includes(asianName.toUpperCase()) && asianName.length > 3) {
            return true;
        }
    }
    
    // Check first name against Asian first names (common patterns)
    const commonAsianFirstNames = ['Wei', 'Jian', 'Ming', 'Feng', 'Hong', 'Hui', 'Jun', 
                                    'Lei', 'Min', 'Ping', 'Qiang', 'Tao', 'Xin', 'Yong', 
                                    'Zhong', 'Chen', 'Lin', 'Hao', 'Kai', 'Li'];
    
    for (const asianFirst of commonAsianFirstNames) {
        if (upperFirstName === asianFirst.toUpperCase()) {
            return true;
        }
    }
    
    // Check for common Asian name patterns (2-3 letter names that are common in Asia)
    if (upperFirstName.length <= 3 && !isCommonWesternName(upperFirstName)) {
        // Short names could be Asian, but we'll be conservative
        return false;
    }
    
    return false;
}

// Common Western names that are short but not Asian
function isCommonWesternName(name) {
    const commonWesternShortNames = new Set([
        'JIM', 'TOM', 'BOB', 'JOE', 'SAM', 'BEN', 'JOHN', 'MIKE', 'DAVE', 'DAN',
        'STEVE', 'PAUL', 'MARK', 'CHRIS', 'SCOTT', 'BRIAN', 'KEVIN', 'TIM', 'RYAN',
        'JACK', 'ADAM', 'ERIC', 'JASON', 'MATT', 'ANDY', 'NICK', 'ALEX', 'LUKE',
        'JAKE', 'JOSH', 'KYLE', 'TYLER', 'JUSTIN', 'BRANDON', 'AARON', 'NATHAN'
    ]);
    return commonWesternShortNames.has(name);
}

// Function to detect if a name is female
function isFemale(firstName) {
    const upperFirstName = firstName?.toUpperCase() || '';
    
    // Check against female names list
    for (const femaleName of femaleNames) {
        if (upperFirstName === femaleName.toUpperCase()) {
            return true;
        }
    }
    
    // Check for common female name endings
    const femaleEndings = ['A', 'IA', 'NA', 'TA', 'LA', 'RA', 'MA', 'SA', 'CA', 'E', 'Y', 'IE'];
    for (const ending of femaleEndings) {
        if (upperFirstName.endsWith(ending) && upperFirstName.length > 2) {
            // Be cautious - not all names ending with 'a' are female
            // Only apply if it's a common pattern
            if (ending === 'A' && !['JOSHUA', 'JONAH', 'ELIJAH', 'ISAIAH', 'JEREMIAH'].includes(upperFirstName)) {
                return true;
            }
        }
    }
    
    return false;
}

// Function to filter developers
function filterDevelopers(developers) {
    const filtered = [];
    const rejected = {
        asianOrigin: [],
        female: [],
        both: []
    };
    
    for (const dev of developers) {
        const firstName = dev.first_name || '';
        const lastName = dev.last_name || '';
        
        // Skip if no name data
        if (!firstName && !lastName) {
            continue;
        }
        
        const isAsian = isAsianOrigin(firstName, lastName);
        const isFemaleName = isFemale(firstName);
        
        // Keep only non-Asian, non-female names
        if (!isAsian && !isFemaleName) {
            filtered.push(dev);
        } else {
            if (isAsian && isFemaleName) {
                rejected.both.push(dev);
            } else if (isAsian) {
                rejected.asianOrigin.push(dev);
            } else if (isFemaleName) {
                rejected.female.push(dev);
            }
        }
    }
    
    return { filtered, rejected };
}

// Process each state
console.log('\n🔍 Filtering developers...\n');
const filteredData = {};
let totalFiltered = 0;
let totalRejected = {
    asianOrigin: 0,
    female: 0,
    both: 0
};

for (const [state, stateData] of Object.entries(allData)) {
    if (!stateData.developers || stateData.developers.length === 0) {
        filteredData[state] = {
            ...stateData,
            developers: [],
            filteredCount: 0,
            rejectedCount: 0
        };
        continue;
    }
    
    const { filtered, rejected } = filterDevelopers(stateData.developers);
    
    filteredData[state] = {
        state: stateData.state,
        originalCount: stateData.developers.length,
        filteredCount: filtered.length,
        rejectedCount: rejected.asianOrigin.length + rejected.female.length + rejected.both.length,
        developers: filtered,
        rejected: {
            asianOrigin: rejected.asianOrigin.length,
            female: rejected.female.length,
            both: rejected.both.length
        },
        timestamp: new Date().toISOString()
    };
    
    totalFiltered += filtered.length;
    totalRejected.asianOrigin += rejected.asianOrigin.length;
    totalRejected.female += rejected.female.length;
    totalRejected.both += rejected.both.length;
    
    console.log(`${state.padEnd(20)}: Original: ${stateData.developers.length.toString().padStart(5)} → Filtered: ${filtered.length.toString().padStart(5)} (Removed: ${(rejected.asianOrigin.length + rejected.female.length + rejected.both.length).toString().padStart(5)})`);
}

// Save filtered data
const outputFile = dataFile.replace('.json', '_filtered.json');
fs.writeFileSync(outputFile, JSON.stringify(filteredData, null, 2));
console.log(`\n✅ Filtered data saved to: ${outputFile}`);

// Save rejected data for review (optional)
const rejectedFile = dataFile.replace('.json', '_rejected.json');
const rejectedData = {
    summary: {
        totalAsianOriginRemoved: totalRejected.asianOrigin,
        totalFemaleRemoved: totalRejected.female,
        totalBothRemoved: totalRejected.both,
        totalRemoved: totalRejected.asianOrigin + totalRejected.female + totalRejected.both,
        totalKept: totalFiltered
    },
    byState: {}
};

for (const [state, stateData] of Object.entries(filteredData)) {
    if (stateData.rejected) {
        rejectedData.byState[state] = stateData.rejected;
    }
}

fs.writeFileSync(rejectedFile, JSON.stringify(rejectedData, null, 2));
console.log(`✅ Rejection summary saved to: ${rejectedFile}`);

// Print summary
console.log('\n' + '='.repeat(60));
console.log('📊 FILTERING SUMMARY');
console.log('='.repeat(60));
console.log(`Total developers before filtering: ${totalBefore}`);
console.log(`Total developers after filtering: ${totalFiltered}`);
console.log(`Total removed: ${totalBefore - totalFiltered}`);
console.log(`\nBreakdown of removed profiles:`);
console.log(`  • Asian origin: ${totalRejected.asianOrigin}`);
console.log(`  • Female names: ${totalRejected.female}`);
console.log(`  • Both Asian & Female: ${totalRejected.both}`);
console.log(`\n📁 Files created:`);
console.log(`  • Filtered data: ${outputFile}`);
console.log(`  • Rejection summary: ${rejectedFile}`);

// Optional: Create a CSV for easier analysis
const csvFile = dataFile.replace('.json', '_filtered.csv');
let csvContent = 'State,User ID,First Name,Last Name,Profile URL\n';
for (const [state, stateData] of Object.entries(filteredData)) {
    for (const dev of stateData.developers) {
        csvContent += `"${state}","${dev.user_id || ''}","${dev.first_name || ''}","${dev.last_name || ''}","${dev.profile_url || ''}"\n`;
    }
}
fs.writeFileSync(csvFile, csvContent);
console.log(`  • CSV export: ${csvFile}`);

console.log('\n✨ Filtering complete!');