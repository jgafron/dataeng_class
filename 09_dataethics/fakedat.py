from faker import Faker
import pandas as pd
import random

# Initialize Faker
fake_data = Faker()
Faker.seed(10)
random.seed(10)

# Predefined salaries
salaries = {
    'Marketing': 78000,
    'Human Resource': 85000,
    'Product': 149000,
    'I/T': 131000,
    'Finance': 129000,
    'Sales': 101420,
    'Operations': 116670,
    'Administrative': 61239,
    'Legal': 90000
}

# Function to assign salary based on department
def assign_salary(department):
    return salaries[department]

# Main function to generate data
def input_data(x):

    # Weighted choices for names and departments
    name_elements = ['en_US', 'en_IN', 'zh_CN', 'en_CA', 'ko_KR', 'en_PH', 'zh_TW', 'es_MX']
    name_margins = [.6, 0.3313, 0.0525, .0044, .0040, .0027, .0027, .0027]
    department_elements = ['Legal', 'Marketing', 'Administrative', 'Operations', 'Sales', 'Finance', 'I/T', 'Product', 'Human Resource']
    department_margins = [0.05, 0.1, .1, .2, .1, .05, .1, .2, .1]
    language_elements = ['Korean', 'Spanish', 'Hindi', 'Arabic', 'Mandarin', 'Tagalog']
    
    # Generate all departments and their respective salaries in advance
    departments = random.choices(population=department_elements, weights=department_margins, k=x)
    salaries = [assign_salary(department) for department in departments]
    
    # Generate basic data
    first_names = []
    last_names = []
    emails = []
    phones = []
    genders = []
    ages = []
    job_titles = []
    years_of_experience = []
    other_languages = []

    for i in range(x):
        name_choice = random.choices(population=name_elements, weights=name_margins)[0]
        other_names = Faker(name_choice)
        
        gender = fake_data.random_element(elements=('male', 'female'))
        if gender == 'male':
            first_name = other_names.first_name_male()
            last_name = other_names.last_name()
        else:
            first_name = other_names.first_name_female()
            last_name = other_names.last_name()
        
        num = random.randint(1, 1000)
        email = f"{first_name.lower()}.{last_name.lower()}{num}@slingacademy.com"
        phone = fake_data.phone_number()
        age = random.randint(18, 70)
        job_title = fake_data.job()
        years_exp = random.randint(1, 15)
        languages = fake_data.random_choices(elements=language_elements, length=random.randint(0, 2))
        languages = ', '.join(languages)
        
        first_names.append(first_name)
        last_names.append(last_name)
        emails.append(email)
        phones.append(phone)
        genders.append(gender.capitalize())
        ages.append(age)
        job_titles.append(job_title)
        years_of_experience.append(years_exp)
        other_languages.append(languages)

        if (i + 1) % 100 == 0:
            print(f"{i + 1} Rows Made")
    
    # Create the DataFrame
    df = pd.DataFrame({
        'First Name': first_names,
        'Last Name': last_names,
        'Email': emails,
        'Phone': phones,
        'Gender': genders,
        'Age': ages,
        'Job Title': job_titles,
        'Years of Experience': years_of_experience,
        'Salary': salaries,
        'Department': departments,
        'Other Languages': other_languages
    })
    
    # Convert columns to appropriate types
    df['Age'] = pd.to_numeric(df['Age'], downcast='integer')
    df['Years of Experience'] = pd.to_numeric(df['Years of Experience'], downcast='integer')
    df['Salary'] = pd.to_numeric(df['Salary'], downcast='integer')

    return df

# Generate data
data = input_data(10000)

# Save to CSV
data.to_csv('fakedata.csv', index=False)
