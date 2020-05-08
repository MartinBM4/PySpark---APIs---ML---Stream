import random


if __name__ == "__main__":
    number_of_points = 10000000
    number_of_features = 4
    number_of_labels = 2 # 2 means binary classification

    probability_of_empty_values = 0.0

    output_file_name ="data/classification." + str(number_of_points)+"."+ str(number_of_features)+".txt"

    random.seed(1)

    with open(output_file_name, "w") as file:
        for point in range(number_of_points):
            file.write(str(random.randrange(0, number_of_labels)) + " ")
            for feature in range(number_of_features):
                if (random.random() > probability_of_empty_values):
                    value = random.random()
                    file.write(str(feature+1)+":"+str(value)+ " ")
            file.write("\n")

    file.close()
